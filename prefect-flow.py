from prefect import flow, task, get_run_logger
from pathlib import Path
import requests
import boto3
import time
import json


# FIRST TASK: Sends a POST request to populate the SQS queue with messages

@task
def populate_sqs_queue():
    logger = get_run_logger()
    url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/mce8ep" # API endpoint with my UVA computing ID
    
    try:
        payload = requests.post(url).json()

        logger.info("Queue populated successfully!")
        logger.info(f"Response data: {payload}")
        return payload["sqs_url"] # returns just the SQS queue URL that will then be needed for monitoring in the next task

    except requests.exceptions.RequestException as e:
        logger.error(f"Error populating SQS queue: {e}")
        return None
        

# SECOND TASK: initializes boto3 SQS client, continuously monitors the SQS queue and processes messages as they become visible

# note - this is my attempt at an innovative design that doesn't wait 900 secs for all messages to become visible and process
# them all at once; instead processes them as they become visible, which makes the monitoring a bit different because messages
# are deleted as soon as they are visible/processed, so number of messages available for pickup should always technically be 0.
# this is reflected in the task with a tracker that instead counts how many messages have been collected/deleted so far, and how
# many messages are left to be collected/haven't become visible yet

@task
def monitor_sqs_queue(queue_url):
    logger = get_run_logger()
    sqs = boto3.client("sqs")
    results = {}

    logger.info("Monitoring queue and processing messages as they become visible")

    # loop until all 21 messages are collected/deleted
    while len(results) < 21:
        
        # getting updated status of messages in the queue
        try:
            attributes = sqs.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=['All']
            )

            attrs = attributes["Attributes"]

            visible = int(attrs.get("ApproximateNumberOfMessages", 0))
            not_visible = int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0))
            delayed = int(attrs.get("ApproximateNumberOfMessagesDelayed", 0))

            # this reflects the total amount of messages that were visible at one point
            num_processed = len(results) + visible + not_visible

            logger.info(f"Approximate number of messages made available/processed so far: {num_processed}")
            logger.info(f"Approximate number of messages that have not been made available for pickup yet: {delayed}\n")

        except Exception as e:
            logger.error(f"Error getting queue attributes: {e}")
            visible = 0

        # receiving and processing messages that are visible
        try:
            response = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=10, # max number of messages SQS allows to retrieve at once, can fetch up to 10 messages at a time
                WaitTimeSeconds=10, # poll every 10 seconds
                MessageAttributeNames=['All'] # retrieve all custom message attributes
            )

            messages = response.get("Messages", []) # get contents of available messages, empty list if none available

            # no new messages available, pause before polling again
            if not messages:
                logger.info("No new messages available, waiting before polling again")
                time.sleep(5)
                continue

            # for messages that are visible
            for msg in messages:
                try:
                    order_no = msg["MessageAttributes"]["order_no"]["StringValue"]
                    word = msg["MessageAttributes"]["word"]["StringValue"]

                    # if message with this order number hasn't been processed yet, store it
                    if order_no not in results:
                        results[order_no] = word
                        logger.info(f"Received and stored message. Order number: '{order_no}' | Word: '{word}'")

                        # persist storage of parsed message content
                        data_path = Path(__file__).parent / "parsed_messages.json"

                        # loading existing data from json file (if it exists)
                        if data_path.exists():
                            try:
                                with open(data_path, "r") as f:
                                    stored_data = json.load(f)
                            except json.JSONDecodeError:
                                stored_data = {}
                        else:
                            stored_data = {}
                        
                        # adding new message only if it’s not already in the file
                        if order_no not in stored_data:
                            stored_data[order_no] = word
                            with open(data_path, "w") as f:
                                json.dump(stored_data, f, indent=2)
                            logger.info(f"Added message {order_no}: '{word}' to {data_path.name}")
                        else:
                            logger.info(f"Message {order_no} already exists in {data_path.name}; skipping write.")

                    else:
                        logger.info(f"Duplicate message {order_no} skipped.")

                    # fetch receipt handle and delete the message once processed
                    sqs.delete_message(
                        QueueUrl=queue_url,
                        ReceiptHandle=msg["ReceiptHandle"]
                    )

                except KeyError as e:
                    logger.error(f"Error processing message due to: {e}")
                    continue

        # error handling in case of null reply to receive_message method
        except Exception as e:
            logger.error(f"Error receiving messages: {e}")
            time.sleep(5)

    logger.info("All 21 messages received and deleted from queue")
    return results    


# THIRD TASK: reassembling the hidden message using the collected words and their order number

@task
def reassemble_message(results):
    logger = get_run_logger()
        
    # error handling for empty results
    if not results:
        logger.info("No results to reassemble.")
        return None
        
    sorted_items = sorted(results.items(), key=lambda x: int(x[0])) # turning results into a list of tuples, then sorting those by the key (order number, which has to be converted to int for proper sorting)
    phrase = " ".join([word for _, word in sorted_items]) # list comprehension, ignore the order number and just extract the words in order to join them into a phrase separated by spaces

    logger.info(f"\nHidden phrase:\n{phrase}\n")

    # persist storage of hidden phrase
    try:
        phrase_path = Path(__file__).parent / "hidden_phrase.json"

        # Only save if file doesn't already exist
        if not phrase_path.exists():
            with open(phrase_path, "w") as f:
                json.dump({"phrase": phrase}, f, indent=2)
            logger.info(f"Hidden phrase saved to {phrase_path.name}")
        else:
            logger.info(f"{phrase_path.name} already exists; skipping write.")

    except Exception as e:
        logger.error(f"Error writing hidden phrase to file: {e}")

    return phrase


# FOURTH TASK: sending properly assembled phrase (and computing ID and platform used) to a separate SQS queue

@task
def send_solution(uvaid, phrase, platform):
    logger = get_run_logger()

    sqs = boto3.client("sqs")
    url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
    message = "Final project submission for Prefect pipeline"

    try:
        response = sqs.send_message(
            QueueUrl=url,
            MessageBody=message,
            MessageAttributes={
                'uvaid': {
                    'DataType': 'String',
                    'StringValue': uvaid
                },
                'phrase': {
                    'DataType': 'String',
                    'StringValue': phrase
                },
                'platform': {
                    'DataType': 'String',
                    'StringValue': platform
                }
            }
        )

        # checking for 200 HTTP response message
        response_message = response['ResponseMetadata']['HTTPStatusCode']
        if response_message == 200:
            logger.info("Submission successful - Server responded with HTTP 200.")
        else:
            logger.info(f"Submission returned non-200 status: {response_message}")

    except Exception as e:
        logger.error(f"Error sending solution: {e}")
        return None


# flow definition

@flow
def main_flow():
    logger = get_run_logger()

    # first task: populate SQS queue
    queue_url = populate_sqs_queue()
    if not queue_url:
        logger.info("Queue could not be populated. Exiting flow.")
        return
    
    # second task: monitor SQS queue and process messages
    results = monitor_sqs_queue(queue_url)
    if not results:
        logger.info("Messages were not properly processed. Exiting flow.")
        return

    # third task: reassemble hidden message
    phrase = reassemble_message(results)
    if not phrase:
        logger.info("Phrase could not be reassembled. Exiting flow.")
        return

    # fourth task: send solution to submission SQS queue
    send_solution("mce8ep", phrase, "prefect")


if __name__ == "__main__":
    main_flow()