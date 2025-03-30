import pika
import os
from PyPDF2 import PdfReader
import io
from pyspark.sql import SparkSession

# RabbitMQ connection details
RABBITMQ_HOST = 'localhost'
RABBITMQ_QUEUE = 'file_queue'
CHUNK_QUEUE = "file_chunks_queue"  # Queue for sending chunks
SUMMARY_QUEUE = "summary_queue"

# Directory to save received files (not used in this version)
OUTPUT_DIR = 'received_files'

# Ensure the output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

# spark = SparkSession.builder \
#     .appName("FileChunker") \
#     .config("spark.python.worker.timeout", "600") \ 
#     .config("spark.executor.memory", "2g") \ 
#     .master("local[*]") \ 
#     .getOrCreate()

spark = SparkSession.builder \
    .appName("FileChunker") \
    .config("spark.python.worker.timeout", "600") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.pyspark.python", "D:\IdeaProjects\python-file-processor\myenv\Scripts\python.exe") \
    .master("local[*]") \
    .getOrCreate()


def send_to_queue(queue_name: str, body: bytes, headers: dict):
    """
    Send a message to a RabbitMQ queue.
    """
    print("Sending chunks to summary queue")
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        channel = connection.channel()
        channel.queue_declare(queue=queue_name, durable=True)
        channel.basic_publish(
            exchange="",
            routing_key=queue_name,
            body=body,
            properties=pika.BasicProperties(headers=headers)
        )
        connection.close()
    except Exception as e:
        print(f"Error sending message to {queue_name}: {e}")

def chunk_file_with_spark(file_bytes, chunk_size=1024 * 1024):  # Default chunk size: 1 MB
    """
    Use Apache Spark to split the file into chunks.
    
    :param file_bytes: The file content as bytes.
    :param chunk_size: Size of each chunk in bytes (default: 1 MB).
    :return: List of chunks.
    """
    # Convert the file bytes into an RDD (Resilient Distributed Dataset)
    rdd = spark.sparkContext.parallelize([file_bytes])

    # Split the file into chunks
    chunks = rdd.flatMap(lambda x: [x[i:i + chunk_size] for i in range(0, len(x), chunk_size)]).collect()

    return chunks

def process_file(file_bytes, file_name):
    """
    Process the file by splitting it into chunks using Spark and printing them.
    
    :param file_bytes: The file content as bytes.
    :param file_name: The name of the file.
    """
    try:
        # Split the file into chunks using Spark
        chunks = chunk_file_with_spark(file_bytes)

        # Print the chunks
        print(f"Processing file: {file_name}")
        for i, chunk in enumerate(chunks):
            print(f"Chunk {i + 1}: {len(chunk)} bytes")  # Print chunk size instead of content for brevity
            send_to_queue(SUMMARY_QUEUE, chunk, {"file-name": file_name, "chunk-number": str(i + 1)})
    except Exception as e:
        print(f"Error processing file {file_name}: {e}")

def callback(ch, method, properties, body):
    """
    Callback function to process messages from RabbitMQ.
    """
    try:
        # Extract the file name from the message headers
        file_name = properties.headers.get('file-name', 'unknown_file')

        # Process the file bytes
        process_file(body, file_name)

        # Acknowledge the message
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"Error processing message: {e}")

def consume_files():
    """
    Consume files from the RabbitMQ queue.
    """
    try:
        # Connect to RabbitMQ
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        channel = connection.channel()

        # Declare the queue
        channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)

       
        channel.basic_consume(queue=RABBITMQ_QUEUE, on_message_callback=callback)

        print("Waiting for files. To exit, press CTRL+C")
        channel.start_consuming()
    except Exception as e:
        print(f"Error in RabbitMQ connection: {e}")
    finally:
        # Close the RabbitMQ connection
        if 'connection' in locals() and connection.is_open:
            connection.close()
        # Stop the Spark session
        spark.stop()

if __name__ == "__main__":
    consume_files()
   