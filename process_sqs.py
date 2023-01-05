import json
import localstack_client.session as boto3
import psycopg2
import psycopg2.extras as extras
import pandas as pd
import datetime
import mmh3

QUEUE_NAME = "login-queue"


def insert_to_database(conn, df, table):
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ",".join(list(df.columns))
    # SQL query to execute
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        raise error
    print("written to database")
    cursor.close()


def transform(raw_message: dict[str, str]) -> dict[str, str]:
    try:
        msg = {
            "user_id": raw_message["user_id"],
            "device_type": raw_message["device_type"],
            "masked_ip": str(mmh3.hash(raw_message["ip"])),
            "masked_device_id": str(mmh3.hash(raw_message["device_id"])),
            "locale": raw_message["locale"],
            "app_version": int(raw_message["app_version"].split(".")[0]),
            "create_date": datetime.datetime.now(),
        }
        return msg
    except Exception as e:
        print(raw_message, e)
        print("Failed to transform")  # write to dead_letter_queuee
        return None


def receive_messages(conn):
    sqs = boto3.client("sqs")
    queue_url = sqs.create_queue(QueueName=QUEUE_NAME)["QueueUrl"]
    print(f"queue_url: [{queue_url}]")
    msgs = []
    message_receipt = {}

    while True:
        received_messages = sqs.receive_message(
            QueueUrl=queue_url,
            AttributeNames=["All"],
            MaxNumberOfMessages=10,
            VisibilityTimeout=2,
            WaitTimeSeconds=2,
        )
        if "Messages" not in received_messages:
            print("finished processing all messages")
            break
        for message in received_messages["Messages"]:
            messages_cleaned = transform(json.loads(message["Body"]))
            if messages_cleaned:
                msgs.append(messages_cleaned)
            message_receipt[message["MessageId"]] = message[
                "ReceiptHandle"
            ]  # only delete message receipt
        df = pd.DataFrame(msgs)
        insert_to_database(conn, df, table="user_logins")
        entries = []
        for k, v in message_receipt.items():
            d = {"Id": k, "ReceiptHandle": v}
            entries.append(d)
        sqs.delete_message_batch(QueueUrl=queue_url, Entries=entries)  # this could fail
        msgs = []

    return


def main():
    conn = psycopg2.connect(
        database="postgres",
        user="postgres",
        password="postgres",
        host="localhost",
        port="5432",
    )
    receive_messages(conn)
    conn.close()


if __name__ == "__main__":
    main()
