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

import os
import time
import psycopg2
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer
from employee import Employee

TOPIC = "bf_employee_cdc"

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "15432")
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PWD  = os.getenv("DB_PWD",  "postgres")


class CDCProducer:
    def __init__(self):
        self.p = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "acks": "all"})
        self.encoder = StringSerializer("utf-8")
        self.last_action_id = 0

    def _connect(self):
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PWD
        )
        conn.autocommit = True
        return conn

    def _init_offset_table(self, cur):
        cur.execute("""
            CREATE TABLE IF NOT EXISTS cdc_offset(
                id INT PRIMARY KEY,
                last_action_id INT
            )
        """)
        cur.execute("""
            INSERT INTO cdc_offset(id,last_action_id)
            VALUES (1,0)
            ON CONFLICT (id) DO NOTHING
        """)

    def load_offset(self):
        conn = self._connect()
        cur = conn.cursor()
        self._init_offset_table(cur)
        cur.execute("SELECT last_action_id FROM cdc_offset WHERE id=1")
        row = cur.fetchone()
        self.last_action_id = row[0] if row else 0
        cur.close()
        conn.close()
        print(f"[offset] loaded last_action_id={self.last_action_id}")

    def save_offset(self, action_id: int):
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("UPDATE cdc_offset SET last_action_id=%s WHERE id=1", (action_id,))
        cur.close()
        conn.close()
        self.last_action_id = action_id
        print(f"[offset] saved last_action_id={self.last_action_id}")

    def fetch_cdc_rows(self):
        conn = self._connect()
        cur = conn.cursor()
        self._init_offset_table(cur)

        cur.execute(
            """
            SELECT action_id, emp_id, first_name, last_name, dob, city, salary, action
            FROM emp_cdc
            WHERE action_id > %s
            ORDER BY action_id ASC
            """,
            (self.last_action_id,),
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows

    def delivery_callback(self, err, msg):
        if err:
            print(f"[kafka] delivery failed: {err}")
        else:
            print(f"[kafka] delivered {msg.topic()}[{msg.partition()}] offset={msg.offset()}")

    def run(self):
        self.load_offset()

        while True:
            rows = self.fetch_cdc_rows()

            if not rows:
                time.sleep(1)
                continue

            print(f"[cdc] fetched {len(rows)} rows")

            last_id = self.last_action_id

            for r in rows:
                action_id = r[0]
                employee = Employee.from_line(r[1:])
                payload = employee.to_json()

                self.p.produce(
                    TOPIC,
                    key=self.encoder(str(employee.emp_id)),
                    value=self.encoder(payload),
                    callback=self.delivery_callback,
                )
                self.p.poll(0)
                last_id = action_id

            self.p.flush()
            self.save_offset(last_id)


if __name__ == "__main__":
    CDCProducer().run()