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

import csv
import json
import os
import time
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer
from employee import Employee
import confluent_kafka
# from pyspark.sql import SparkSession
import pandas as pd
import psycopg2

# Kafka topic names
employee_topic_name = "bf_employee_cdc"
dlq_topic_name = "bf_employee_cdc_dlq"

class cdcProducer(Producer):
    """
    CDC Producer class that reads change data capture records from PostgreSQL
    and publishes them to Kafka topics. Includes DLQ handling for failed messages.
    
    Network Configuration:
    - If running outside Docker: host = localhost, port = 29092
    - If running inside Docker: host = 'kafka', port = 9092
    """
    
    def __init__(self, host="localhost", port="29092"):
        """
        Initialize the CDC Producer.
        
        Args:
            host (str): Kafka broker hostname
            port (str): Kafka broker port
        """
        self.host = host
        self.port = port
        # Producer configuration with acknowledgment settings
        producerConfig = {
            'bootstrap.servers': f"{self.host}:{self.port}",
            'acks': 'all',  # Wait for all in-sync replicas to acknowledge
            'retries': 3,   # Retry failed sends up to 3 times
            'max.in.flight.requests.per.connection': 1  # Ensure ordering
        }
        super().__init__(producerConfig)
        self.running = True
        self.last_action_id = 0
        self.dlq_producer = None  # Separate producer for DLQ messages

    def load_offset(self, cur):
        """
        Load the last processed action_id from the offset table.
        This allows the producer to resume from where it left off after restarts.
        
        Args:
            cur: PostgreSQL cursor object
        """
        try:
            cur.execute('SELECT last_action_id FROM cdc_offset WHERE id = 1')
            result = cur.fetchone()
            if result:
                self.last_action_id = result[0]
                print(f'Loaded offset: {self.last_action_id}')
            else:
                # If no offset exists, start from the beginning
                self.last_action_id = 0
        except Exception as e:
            print(f'Error loading offset: {e}')
            # Default to 0 if offset table doesn't exist or has issues
            self.last_action_id = 0

    def save_offset(self, cur, action_id):
        """
        Save the last processed action_id to the offset table.
        This ensures we can resume processing after restarts.
        
        Args:
            cur: PostgreSQL cursor object
            action_id (int): The action_id that was successfully processed
        """
        try:
            cur.execute('UPDATE cdc_offset SET last_action_id = %s WHERE id = 1', (action_id,))
            self.last_action_id = action_id
            print(f'Saved offset: {self.last_action_id}')
        except Exception as e:
            print(f'Error saving offset: {e}')
    
    def fetch_cdc(self):
        """
        Fetch new CDC records from the source database that haven't been processed yet.
        Uses the last_action_id to only fetch records newer than what we've already processed.
        
        Returns:
            list: List of tuples containing CDC records, or empty list if none found
        """
        records = []
        try:
            # Connect to source PostgreSQL database
            conn = psycopg2.connect(
                host="localhost",
                database="postgres",
                user="postgres",
                port='5432',
                password="postgres"
            )
            conn.autocommit = True
            cur = conn.cursor()
            
            # Query for new CDC records that haven't been processed
            # Ordered by action_id to ensure sequential processing
            cur.execute("""
                SELECT action_id, emp_id, first_name, last_name, dob, city, salary, action
                FROM emp_cdc 
                WHERE action_id > %s
                ORDER BY action_id ASC
            """, (self.last_action_id,))
            
            records = cur.fetchall()
            cur.close()
            conn.close()
            
            if records:
                print(f'Fetched {len(records)} new CDC records.')
                # Update last_action_id to the highest fetched record
                self.last_action_id = records[-1][0]
            else:
                print('No new CDC records found.')

        except Exception as err:
            print(f'Error fetching CDC data: {err}')
            records = []
        
        return records
    
    def delivery_callback(self, err, msg):
        """
        Callback function called when a message delivery is completed or fails.
        This is used to handle delivery acknowledgments and errors.
        
        Args:
            err: Error object if delivery failed, None if successful
            msg: The message object that was delivered (or failed)
        """
        if err is not None:
            # Message delivery failed - send to DLQ
            print(f'Message delivery failed: {err}')
            self.send_to_dlq(msg, err)
        else:
            # Message successfully delivered
            print(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')
    
    def send_to_dlq(self, msg, error):
        """
        Send a failed message to the Dead Letter Queue (DLQ) topic.
        This ensures failed messages are not lost and can be reprocessed later.
        
        Args:
            msg: The original message that failed
            error: The error that caused the failure
        """
        try:
            # Create DLQ producer if it doesn't exist
            if self.dlq_producer is None:
                dlq_config = {
                    'bootstrap.servers': f"{self.host}:{self.port}",
                    'acks': 'all'
                }
                self.dlq_producer = Producer(dlq_config)
            
            # Create DLQ message with error information
            dlq_message = {
                'original_topic': msg.topic() if msg else 'unknown',
                'original_key': msg.key().decode('utf-8') if msg and msg.key() else None,
                'original_value': msg.value().decode('utf-8') if msg and msg.value() else None,
                'error': str(error),
                'timestamp': time.time()
            }
            
            # Send to DLQ topic
            self.dlq_producer.produce(
                dlq_topic_name,
                key=msg.key() if msg and msg.key() else None,
                value=json.dumps(dlq_message).encode('utf-8'),
                callback=lambda err, msg: print(f'DLQ message sent: {err if err else "success"}')
            )
            self.dlq_producer.poll(0)  # Trigger delivery callbacks
            print(f'Message sent to DLQ: {dlq_topic_name}')
            
        except Exception as e:
            print(f'Error sending message to DLQ: {e}')
    
    def publish_cdc_records(self, records):
        """
        Publish CDC records to Kafka topic.
        
        Args:
            records (list): List of CDC record tuples from the database
            
        Returns:
            bool: True if all records were published successfully, False otherwise
        """
        if not records:
            return True
        
        success_count = 0
        try:
            # Connect to database for offset tracking
            conn = psycopg2.connect(
                host="localhost",
                database="postgres",
                user="postgres",
                port='5432',
                password="postgres"
            )
            conn.autocommit = True
            cur = conn.cursor()
            
            # Process each CDC record
            for record in records:
                try:
                    # Parse record tuple into Employee object
                    # Format: (action_id, emp_id, first_name, last_name, dob, city, salary, action)
                    action_id, emp_id, first_name, last_name, dob, city, salary, action = record
                    
                    # Create Employee object
                    employee = Employee(
                        action_id=action_id,
                        emp_id=emp_id,
                        emp_FN=first_name,
                        emp_LN=last_name,
                        emp_dob=str(dob),
                        emp_city=city,
                        emp_salary=salary,
                        action=action
                    )
                    
                    # Serialize employee to JSON
                    employee_json = employee.to_json()
                    
                    # Publish to Kafka topic with delivery callback
                    # Use emp_id as key to ensure records for same employee go to same partition
                    self.produce(
                        employee_topic_name,
                        key=str(emp_id).encode('utf-8'),
                        value=employee_json.encode('utf-8'),
                        callback=self.delivery_callback
                    )
                    
                    # Poll to trigger delivery callbacks
                    self.poll(0)
                    
                    success_count += 1
                    print(f'Published CDC record: action_id={action_id}, emp_id={emp_id}, action={action}')
                    
                except Exception as e:
                    print(f'Error publishing record {record}: {e}')
                    # Send failed record to DLQ
                    try:
                        failed_msg = type('obj', (object,), {
                            'topic': lambda: employee_topic_name,
                            'key': lambda: str(record[1]).encode('utf-8') if len(record) > 1 else None,
                            'value': lambda: json.dumps(record).encode('utf-8')
                        })()
                        self.send_to_dlq(failed_msg, e)
                    except:
                        pass
            
            # Flush all pending messages to ensure delivery
            self.flush(timeout=10)
            
            # If DLQ producer exists, flush it too
            if self.dlq_producer:
                self.dlq_producer.flush(timeout=10)
            
            # Save offset only if all records were processed successfully
            if success_count == len(records):
                self.save_offset(cur, self.last_action_id)
                print(f'Successfully published {success_count} records. Offset saved: {self.last_action_id}')
            else:
                print(f'Warning: Only {success_count} out of {len(records)} records were published successfully.')
            
            cur.close()
            conn.close()
            
        except Exception as e:
            print(f'Error in publish_cdc_records: {e}')
            return False
        
        return success_count == len(records)
    

if __name__ == '__main__':
    """
    Main execution loop for the CDC Producer.
    Continuously polls the source database for new CDC records and publishes them to Kafka.
    """
    # Initialize the producer
    producer = cdcProducer()
    
    # Poll interval in seconds (how often to check for new CDC records)
    POLL_INTERVAL = 5
    
    print('CDC Producer started. Polling for new records...')
    print(f'Kafka broker: {producer.host}:{producer.port}')
    print(f'Topic: {employee_topic_name}')
    print(f'DLQ Topic: {dlq_topic_name}')
    
    try:
        while producer.running:
            # Fetch new CDC records from source database
            records = producer.fetch_cdc()
            
            if records:
                # Publish records to Kafka
                producer.publish_cdc_records(records)
            else:
                # No new records, wait before next poll
                print(f'No new records. Waiting {POLL_INTERVAL} seconds...')
            
            # Wait before next poll cycle
            time.sleep(POLL_INTERVAL)
            
    except KeyboardInterrupt:
        print('\nShutting down producer...')
        producer.running = False
        # Flush any remaining messages
        producer.flush(timeout=10)
        if producer.dlq_producer:
            producer.dlq_producer.flush(timeout=10)
        print('Producer stopped.')
    except Exception as e:
        print(f'Fatal error in producer: {e}')
        producer.running = False
    
