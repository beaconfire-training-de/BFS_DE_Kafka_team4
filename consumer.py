"""
Copyright (C) 2024 BeaconFire Staffing Solutions
Author: Ray Wang

This file is part of Oct DE Batch Kafka Project 1 Assignment.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
"""


import json
import time
import psycopg2
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from confluent_kafka.serialization import StringDeserializer
from employee import Employee
from producer import employee_topic_name

# DLQ topic name for failed messages
dlq_topic_name = "bf_employee_cdc_dlq"

class cdcConsumer(Consumer):
    """
    CDC Consumer class that consumes messages from Kafka and applies changes
    to the destination database. Includes DLQ handling for failed message processing.
    
    Network Configuration:
    - If running outside Docker: host = localhost, port = 29092
    - If running inside Docker: host = 'kafka', port = 9092
    """
    
    def __init__(self, host: str = "localhost", port: str = "29092", group_id: str = ''):
        """
        Initialize the CDC Consumer.
        
        Args:
            host (str): Kafka broker hostname
            port (str): Kafka broker port
            group_id (str): Consumer group ID for load balancing and offset management
        """
        # Consumer configuration
        self.conf = {
            'bootstrap.servers': f'{host}:{port}',
            'group.id': group_id,
            'enable.auto.commit': True,  # Automatically commit offsets after processing
            'auto.offset.reset': 'earliest',  # Start from earliest if no offset exists
            'session.timeout.ms': 30000,  # Session timeout
            'max.poll.interval.ms': 300000  # Max time between polls
        }
        super().__init__(self.conf)
        self.keep_runnning = True
        self.group_id = group_id
        self.dlq_producer = None  # Producer for sending failed messages to DLQ
        self.max_retries = 3  # Maximum number of retries before sending to DLQ

    def consume(self, topics, processing_func):
        """
        Main consumption loop that subscribes to topics and processes messages.
        
        Args:
            topics (list): List of topic names to subscribe to
            processing_func (callable): Function to process each message
        """
        try:
            # Subscribe to the specified topics
            self.subscribe(topics)
            print(f'Subscribed to topics: {topics}')
            print(f'Consumer group: {self.group_id}')
            
            # Main consumption loop
            while self.keep_runnning:
                # Poll for messages (timeout: 1 second)
                msg = self.poll(1.0)

                if msg is None:
                    # No message available, continue polling
                    continue
                
                # Check for errors
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition - this is normal, continue
                        continue
                    else:
                        # Other error - raise exception
                        raise KafkaException(msg.error())
                else:
                    # Process the message
                    processing_func(msg)

        except KeyboardInterrupt:
            print('\nShutting down consumer...')
            self.keep_runnning = False
        except Exception as e:
            print(f'Error in consumer loop: {e}')
            self.keep_runnning = False
        finally:
            # Clean up resources
            self.close()
            if self.dlq_producer:
                self.dlq_producer.flush(timeout=10)
            print('Consumer closed.')

def send_to_dlq(consumer, msg, error, original_value=None):
    """
    Send a failed message to the Dead Letter Queue (DLQ) topic.
    This ensures failed messages are not lost and can be reprocessed later.
    
    Args:
        consumer: The consumer instance (to access DLQ producer)
        msg: The Kafka message that failed
        error: The error that caused the failure
        original_value: The original message value (if available)
    """
    try:
        # Create DLQ producer if it doesn't exist
        if consumer.dlq_producer is None:
            dlq_config = {
                'bootstrap.servers': consumer.conf['bootstrap.servers'],
                'acks': 'all'
            }
            consumer.dlq_producer = Producer(dlq_config)
        
        # Create DLQ message with error information
        dlq_message = {
            'original_topic': msg.topic(),
            'original_key': msg.key().decode('utf-8') if msg.key() else None,
            'original_value': original_value if original_value else (msg.value().decode('utf-8') if msg.value() else None),
            'error': str(error),
            'partition': msg.partition(),
            'offset': msg.offset(),
            'timestamp': time.time()
        }
        
        # Send to DLQ topic
        consumer.dlq_producer.produce(
            dlq_topic_name,
            key=msg.key() if msg.key() else None,
            value=json.dumps(dlq_message).encode('utf-8'),
            callback=lambda err, msg: print(f'DLQ message sent: {err if err else "success"}')
        )
        consumer.dlq_producer.poll(0)  # Trigger delivery callbacks
        print(f'Message sent to DLQ: {dlq_topic_name}')
        
    except Exception as e:
        print(f'Error sending message to DLQ: {e}')


# Global variable to hold consumer instance (used by update_dst)
_current_consumer = None


def update_dst(msg):
    """
    Process a CDC message and apply the change to the destination database.
    Handles INSERT, UPDATE, and DELETE operations based on the action type.
    Sends failed messages to DLQ after retries.
    
    Args:
        msg: Kafka message object containing the CDC record
    """
    global _current_consumer
    retry_count = 0
    max_retries = 3
    consumer = _current_consumer
    
    while retry_count <= max_retries:
        try:
            # Parse the message value (JSON string) into Employee object
            message_value = msg.value().decode('utf-8')
            employee_data = json.loads(message_value)
            e = Employee(**employee_data)
            
            print(f"Consuming: action_id={e.action_id}, emp_id={e.emp_id}, action={e.action}")
            print(f"Message details: topic={msg.topic()}, partition={msg.partition()}, offset={msg.offset()}")
            
            # Connect to destination PostgreSQL database
            # Port 5433 is mapped from container port 5432 in docker-compose
            conn = psycopg2.connect(
                host="localhost",
                database="postgres",
                user="postgres",
                port='5433',  # Destination database port
                password="postgres"
            )
            conn.autocommit = True
            cur = conn.cursor()
            
            # Apply the change based on action type
            if e.action == 'INSERT':
                # Insert new employee record
                # ON CONFLICT DO NOTHING prevents errors if record already exists
                cur.execute("""
                    INSERT INTO employees(emp_id, first_name, last_name, dob, city, salary)
                    VALUES (%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (emp_id) DO NOTHING
                """, (e.emp_id, e.emp_FN, e.emp_LN, e.emp_dob, e.emp_city, e.emp_salary))
                print(f'INSERT operation completed for emp_id={e.emp_id}')

            elif e.action == 'UPDATE':
                # Update existing employee record
                cur.execute("""
                    UPDATE employees
                    SET first_name=%s,
                        last_name=%s,
                        dob=%s,
                        city=%s,
                        salary=%s
                    WHERE emp_id=%s
                """, (e.emp_FN, e.emp_LN, e.emp_dob, e.emp_city, e.emp_salary, e.emp_id))
                print(f'UPDATE operation completed for emp_id={e.emp_id}')

            elif e.action == 'DELETE':
                # Delete employee record
                cur.execute("DELETE FROM employees WHERE emp_id=%s", (e.emp_id,))
                print(f'DELETE operation completed for emp_id={e.emp_id}')
            
            else:
                raise ValueError(f'Unknown action type: {e.action}')

            # Close database connection
            cur.close()
            conn.close()
            
            # Success - exit retry loop
            print(f'Successfully processed message: emp_id={e.emp_id}, action={e.action}')
            return
            
        except json.JSONDecodeError as err:
            # JSON parsing error - send to DLQ immediately (no retry)
            print(f'JSON decode error: {err}')
            if consumer:
                send_to_dlq(consumer, msg, err, message_value if 'message_value' in locals() else None)
            else:
                print('Warning: Cannot send to DLQ - consumer instance not available')
            return
            
        except psycopg2.Error as err:
            # Database error - retry
            retry_count += 1
            print(f'Database error (attempt {retry_count}/{max_retries + 1}): {err}')
            
            if retry_count > max_retries:
                # Max retries exceeded - send to DLQ
                print(f'Max retries exceeded. Sending to DLQ.')
                if consumer:
                    send_to_dlq(consumer, msg, err, message_value if 'message_value' in locals() else None)
                else:
                    print('Warning: Cannot send to DLQ - consumer instance not available')
                return
            
            # Wait before retry (exponential backoff)
            time.sleep(2 ** retry_count)
            
        except Exception as err:
            # Other errors - retry
            retry_count += 1
            print(f'Error processing message (attempt {retry_count}/{max_retries + 1}): {err}')
            
            if retry_count > max_retries:
                # Max retries exceeded - send to DLQ
                print(f'Max retries exceeded. Sending to DLQ.')
                if consumer:
                    send_to_dlq(consumer, msg, err, message_value if 'message_value' in locals() else None)
                else:
                    print('Warning: Cannot send to DLQ - consumer instance not available')
                return
            
            # Wait before retry (exponential backoff)
            time.sleep(2 ** retry_count)

def create_processing_function(consumer_instance):
    """
    Create a processing function that has access to the consumer instance.
    This allows the processing function to send messages to DLQ if needed.
    
    Args:
        consumer_instance: The consumer instance
        
    Returns:
        callable: Processing function that can access the consumer
    """
    def process_with_consumer(msg):
        # Set global consumer reference for update_dst function
        global _current_consumer
        _current_consumer = consumer_instance
        update_dst(msg)
    
    return process_with_consumer


if __name__ == '__main__':
    """
    Main execution for the CDC Consumer.
    Creates a consumer instance and starts consuming messages from Kafka.
    """
    # Initialize consumer with group ID for load balancing
    consumer = cdcConsumer(group_id="bf_group")
    
    print('CDC Consumer started.')
    print(f'Kafka broker: {consumer.conf["bootstrap.servers"]}')
    print(f'Consumer group: {consumer.group_id}')
    print(f'Topic: {employee_topic_name}')
    print(f'DLQ Topic: {dlq_topic_name}')
    
    # Create processing function with consumer reference
    processing_func = create_processing_function(consumer)
    
    # Start consuming messages
    consumer.consume([employee_topic_name], processing_func)
import random
import string
import sys
import psycopg2
from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.serialization import StringDeserializer
from employee import Employee
from employee import Employee
from producer import employee_topic_name

class cdcConsumer(Consumer):
    #if running outside Docker (i.e. producer is NOT in the docer-compose file): host = localhost and port = 29092
    #if running inside Docker (i.e. producer IS IN the docer-compose file), host = 'kafka' or whatever name used for the kafka container, port = 9092
    def __init__(self, host: str = "localhost", port: str = "29092", group_id: str = ''):
        self.conf = {'bootstrap.servers': f'{host}:{port}',
                     'group.id': group_id,
                     'enable.auto.commit': True,
                     'auto.offset.reset': 'earliest'}
        super().__init__(self.conf)
        self.keep_runnning = True
        self.group_id = group_id

    def consume(self, topics, processing_func):
        try:
            self.subscribe(topics)
            print(f'Consumer subsribed to topics: {topics}')
            while self.keep_runnning:
                msg = self.poll(timeout=1.0)
                print(msg)
                if msg is None:
                    print('check')
                    continue
                elif msg.error():
                    raise KafkaException(msg.error())
                else:
                    print('uh')
                    processing_func(msg)
                    print('works')
        finally:
            self.close()

def update_dst(msg):
    e = Employee(**(json.loads(msg.value())))
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="postgres",
            user="postgres",
            port = '5433', 
            password="postgres")
        conn.autocommit = True
        cur = conn.cursor()
        if e.action == 'INSERT':
            cur.execute(
                    """
                    INSERT INTO employees (emp_id, first_name, last_name, dob, city, salary)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                        e.emp_id, e.first_name, e.last_name, e.dob, e.city, e.salary,
                    ))
            print(f'Inserted Employee: {e.emp_id}')
        elif e.action == 'UPDATE':
            cur.execute(
                    """
                    UPDATE employees
                    SET first_name = %s, last_name = %s, dob = %s, city = %s, salary = %s
                    WHERE emp_id = %s
                    """, (
                        e.first_name, e.last_name, e.dob, e.city, e.salary, e.emp_id, 
                    ))
            print(f'Updated Employee: {e.emp_id}')

        elif e.action == 'DELETE':
            cur.execute(
                    """
                    DELETE FROM employees
                    WHERE emp_id = %s
                    """, (
                        e.emp_id, 
                    ))
            print(f'Deleted Employee: {e.emp_id}')
        
        cur.close()
        conn.close()
    except Exception as err:
        print(err)

if __name__ == '__main__':
    consumer = cdcConsumer(group_id='employee-migration-v2') 
    consumer.consume([employee_topic_name], update_dst)
