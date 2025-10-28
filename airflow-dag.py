from airflow.decorators import dag, task
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime, timedelta
from pathlib import Path
import requests
import boto3
import json
import time

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

# Define the DAG
@dag(
    dag_id="sqs_pipeline_dag",
    default_args=default_args,
    description="Data pipeline to populate, monitor, process, and submit SQS messages",
    start_date=datetime(2025, 10, 28),
    schedule=None,  # manual trigger only
    catchup=False,
    tags=["sqs", "aws", "api", "pipeline"],
)
def sqs_pipeline():

    @task()
    def populate_sqs_queue():
        log = LoggingMixin().log
        url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/mce8ep"
        log.info("Sending POST request to populate SQS queue...")

        try:
            payload = requests.post(url).json()
            log.info(f"Queue populated successfully: {payload}")
            return payload["sqs_url"]
        except requests.exceptions.RequestException as e:
            log.error(f"Error populating SQS queue: {e}")
            return None

    @task()
    def monitor_sqs_queue(queue_url: str):
        log = LoggingMixin().log
        sqs = boto3.client("sqs", region_name="us-east-1")
        results = {}
        data_path = Path("/opt/airflow/dags/parsed_messages.json")

        log.info("Monitoring SQS queue and processing messages as they become visible...")

        while len(results) < 21:
            try:
                attributes = sqs.get_queue_attributes(
                    QueueUrl=queue_url,
                    AttributeNames=["All"],
                )
                attrs = attributes["Attributes"]
                visible = int(attrs.get("ApproximateNumberOfMessages", 0))
                not_visible = int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0))
                delayed = int(attrs.get("ApproximateNumberOfMessagesDelayed", 0))
                log.info(
                    f"Queue status — Visible: {visible}, NotVisible: {not_visible}, Delayed: {delayed}"
                )
            except Exception as e:
                log.error(f"Error getting queue attributes: {e}")
                visible = 0

            try:
                response = sqs.receive_message(
                    QueueUrl=queue_url,
                    MaxNumberOfMessages=10,
                    WaitTimeSeconds=10,
                    MessageAttributeNames=["All"],
                )
                messages = response.get("Messages", [])
                if not messages:
                    log.info("No new messages. Waiting before polling again...")
                    time.sleep(5)
                    continue

                for msg in messages:
                    try:
                        order_no = msg["MessageAttributes"]["order_no"]["StringValue"]
                        word = msg["MessageAttributes"]["word"]["StringValue"]

                        if order_no not in results:
                            results[order_no] = word

                            # Load existing data
                            if data_path.exists():
                                try:
                                    with open(data_path, "r") as f:
                                        stored_data = json.load(f)
                                except json.JSONDecodeError:
                                    stored_data = {}
                            else:
                                stored_data = {}

                            # Persist if new
                            if order_no not in stored_data:
                                stored_data[order_no] = word
                                with open(data_path, "w") as f:
                                    json.dump(stored_data, f, indent=2)
                                log.info(f"Added message {order_no}: '{word}'")
                            else:
                                log.info(f"Message {order_no} already stored.")
                        else:
                            log.info(f"Duplicate message {order_no} skipped.")

                        sqs.delete_message(
                            QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"]
                        )

                    except KeyError as e:
                        log.error(f"Malformed message: {e}")
                        continue

            except Exception as e:
                log.error(f"Error receiving messages: {e}")
                time.sleep(5)

        log.info("All 21 messages processed and deleted from the queue.")
        return results

    @task()
    def reassemble_message(results: dict):
        log = LoggingMixin().log

        if not results:
            log.warning("No results to reassemble.")
            return None

        sorted_items = sorted(results.items(), key=lambda x: int(x[0]))
        phrase = " ".join([word for _, word in sorted_items])
        log.info(f"Hidden phrase successfully reconstructed:\n{phrase}")

        phrase_path = Path("/opt/airflow/dags/hidden_phrase.json")
        if not phrase_path.exists():
            with open(phrase_path, "w") as f:
                json.dump({"phrase": phrase}, f, indent=2)
            log.info(f"Phrase saved to {phrase_path.name}")
        else:
            log.info(f"{phrase_path.name} already exists; skipping write.")

        return phrase

    @task()
    def send_solution(uvaid: str, phrase: str, platform: str):
        log = LoggingMixin().log
        sqs = boto3.client("sqs", region_name="us-east-1")
        url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
        message = "Final project submission for Airflow pipeline"

        try:
            response = sqs.send_message(
                QueueUrl=url,
                MessageBody=message,
                MessageAttributes={
                    "uvaid": {"DataType": "String", "StringValue": uvaid},
                    "phrase": {"DataType": "String", "StringValue": phrase},
                    "platform": {"DataType": "String", "StringValue": platform},
                },
            )
            code = response["ResponseMetadata"]["HTTPStatusCode"]
            if code == 200:
                log.info("Submission successful — server responded with HTTP 200.")
            else:
                log.warning(f"Submission returned non-200 status: {code}")
        except Exception as e:
            log.error(f"Error submitting solution: {e}")

    # DAG flow definition
    queue_url = populate_sqs_queue()
    results = monitor_sqs_queue(queue_url)
    phrase = reassemble_message(results)
    send_solution("mce8ep", phrase, "airflow")


# Instantiate the DAG
sqs_pipeline_dag = sqs_pipeline()