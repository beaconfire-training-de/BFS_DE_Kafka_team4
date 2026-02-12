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
from confluent_kafka import Producer
from employee import Employee
import confluent_kafka
# from pyspark.sql import SparkSession
import pandas as pd
from confluent_kafka.serialization import StringSerializer
import psycopg2

employee_topic_name = "bf_employee_cdc"

class cdcProducer(Producer):
    #if running outside Docker (i.e. producer is NOT in the docer-compose file): host = localhost and port = 29092
    #if running inside Docker (i.e. producer IS IN the docer-compose file), host = 'kafka' or whatever name used for the kafka container, port = 9092
    def __init__(self, host="localhost", port="29092"):
        self.host = host
        self.port = port
        producerConfig = {'bootstrap.servers':f"{self.host}:{self.port}",
                          'acks' : 'all'}
        super().__init__(producerConfig)
        self.running = True
        self.last_action_id = 0

    def load_offset(self, cur):
        try:
            cur.execute('SELECT last_action_id FROM cdc_offset WHERE id = 1')
            result = cur.fetchone()
            if result:
                self.last_action_id = result[0]
                print(f'Loaded offset: {self.last_action_id}')

        except Exception as e:
            print(f'Error loading offset: {e}')
            #double check this
            self.last_action_id = 0

    def save_offset(self, cur, action_id):
        try:
            cur.execute('UPDATE cdc_offset SET last_action_id = %s WHERE id = 1', (action_id,))
            self.last_action_id = action_id
            print(f'New Offset saved')

        except Exception as e:
            print(f'Error saving offset: {e}')
    
    def fetch_cdc(self,):
        try:
            records = []
            conn = psycopg2.connect(
                host="localhost",
                database="postgres",
                user="postgres",
                port = '5432',
                password="postgres")
            conn.autocommit = True
            cur = conn.cursor()
            conn.autocommit = True
            cur = conn.cursor()
            self.load_offset(cur)
            cur.execute("""
                SELECT action_id, first_name, last_name, dob, city, salary, action, created_at
                        FROM emp_cdc 
                WHERE action_id > %s
            """, (
                self.last_action_id,
            ))
            records = cur.fetchall()
            if records:
                print(f'Fetched {len(records)} new CDC records.')
                last_action_id = records[-1][0]
                self.save_offset(cur, last_action_id)
            cur.close()
            conn.close()

        except Exception as err:
            print(f'Error fetching CDC data: {err}')
        
        return records
    
    def delivery_callback(self, err, msg):
        if err:
            print(f'Message delivery failed: {err}')
        else:
            print(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

if __name__ == '__main__':
    encoder = StringSerializer('utf-8')
    producer = cdcProducer()
    
    while producer.running:
        try:
            cdc_records = producer.fetch_cdc()

            for record in cdc_records:
                employee = Employee.from_line(record)
                message = employee.to_json()
                producer.produce(
                    topic= employee_topic_name,
                    key = encoder(str(employee.emp_id)),
                    value = encoder(message),
                    callback= producer.delivery_callback
                )
                producer.poll(0)
                print(f'Completed {employee.action} for emp_id: {employee.emp_id}')
            producer.flush()
        except Exception as e:
            print(f'Encountered error: {e}')
            print("Actual DB error:", repr(e))
    
