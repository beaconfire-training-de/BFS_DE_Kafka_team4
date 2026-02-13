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
import logging
from contextlib import closing

import psycopg2
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer

from employee import Employee

TOPIC = os.getenv("TOPIC", "bf_employee_cdc")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "15432")
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PWD") or os.getenv("DB_PASSWORD", "postgres")

POLL_INTERVAL_SEC = float(os.getenv("POLL_INTERVAL_SEC", "1"))
BATCH_FLUSH_SIZE = int(os.getenv("BATCH_FLUSH_SIZE", "200"))  # flush every N messages in a batch (0 = only flush once per batch)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger("cdc_producer")


class CDCProducer:
    def __init__(self):
        self.p = Producer({
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "acks": "all",
            "enable.idempotence": True,
            "retries": 5,
            "retry.backoff.ms": 300
        })
        self.encoder = StringSerializer("utf-8")
        self.last_action_id = 0
        self.had_delivery_error = False
        self.running = True

    def _connect(self):
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
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
        with closing(self._connect()) as conn:
            with conn.cursor() as cur:
                self._init_offset_table(cur)
                cur.execute("SELECT last_action_id FROM cdc_offset WHERE id=1")
                row = cur.fetchone()
                self.last_action_id = row[0] if row else 0
        logger.info("Loaded offset last_action_id=%s", self.last_action_id)

    def save_offset(self, action_id: int):
        with closing(self._connect()) as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE cdc_offset SET last_action_id=%s WHERE id=1", (action_id,))
        self.last_action_id = action_id
        logger.info("Saved offset last_action_id=%s", self.last_action_id)

    def fetch_cdc_rows(self):
        with closing(self._connect()) as conn:
            with conn.cursor() as cur:
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
        return rows

    def delivery_callback(self, err, msg):
        if err:
            self.had_delivery_error = True
            logger.error("Kafka delivery failed: %s", err)
        else:
            logger.info("Delivered %s[%s] offset=%s", msg.topic(), msg.partition(), msg.offset())

    def _produce_batch(self, rows):
        """
        Produce a batch and return the last action_id attempted.
        Only save offset if no delivery errors happened.
        """
        if not rows:
            return None

        self.had_delivery_error = False
        last_id = self.last_action_id

        for i, r in enumerate(rows, start=1):
            action_id = r[0]
            employee = Employee.from_line(r[1:])  # emp_id..action
            payload = employee.to_json()

            self.p.produce(
                TOPIC,
                key=self.encoder(str(employee.emp_id)),
                value=self.encoder(payload),
                callback=self.delivery_callback,
            )
            self.p.poll(0)
            last_id = action_id

            if BATCH_FLUSH_SIZE > 0 and i % BATCH_FLUSH_SIZE == 0:
                self.p.flush()

        self.p.flush()
        return last_id

    def run(self):
        self.load_offset()
        logger.info("Producer started (kafka=%s, db=%s:%s, topic=%s)", KAFKA_BOOTSTRAP, DB_HOST, DB_PORT, TOPIC)

        try:
            while self.running:
                rows = self.fetch_cdc_rows()

                if not rows:
                    time.sleep(POLL_INTERVAL_SEC)
                    continue

                logger.info("Fetched %d CDC rows (from action_id>%s)", len(rows), self.last_action_id)

                last_id = self._produce_batch(rows)

                if last_id is not None:
                    if not self.had_delivery_error:
                        self.save_offset(last_id)
                    else:
                        logger.warning("Not saving offset due to Kafka delivery errors. Will retry next loop.")
                        time.sleep(POLL_INTERVAL_SEC)

        except KeyboardInterrupt:
            logger.info("Stopping producer (KeyboardInterrupt).")
        finally:
            try:
                self.p.flush(5)
            except Exception:
                pass
            logger.info("Producer stopped.")


if __name__ == "__main__":
    CDCProducer().run()