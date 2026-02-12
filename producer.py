import csv
import json
import time
from confluent_kafka import Producer
from employee import Employee
from confluent_kafka.serialization import StringSerializer
import psycopg2

employee_topic_name = "bf_employee_cdc"
csv_file = 'employees.csv'


class cdcProducer(Producer):
    def __init__(self, host="localhost", port="29092"):
        producerConfig = {'bootstrap.servers': f"{host}:{port}", 'acks': 'all'}
        super().__init__(producerConfig)
        self.running = True
        self.last_action_id = 0

    # --- OFFSET HANDLING ---
    def load_offset(self, cur):
        try:
            cur.execute('SELECT last_action_id FROM cdc_offset WHERE id = 1')
            result = cur.fetchone()
            if result:
                self.last_action_id = result[0]
            else:
                self.last_action_id = 0
        except Exception as e:
            print(f'Error loading offset: {e}')
            self.last_action_id = 0

    def save_offset(self, cur, action_id):
        try:
            cur.execute('UPDATE cdc_offset SET last_action_id = %s WHERE id = 1', (action_id,))
            self.last_action_id = action_id
        except Exception as e:
            print(f'Error saving offset: {e}')

    def fetch_cdc(self):
        try:
            conn = psycopg2.connect(
                host="localhost",
                database="postgres",
                user="postgres",
                port='5432',
                password="postgres"
            )
            conn.autocommit = True
            cur = conn.cursor()

            # Create offset table if missing
            cur.execute("""
            CREATE TABLE IF NOT EXISTS cdc_offset(
                id INT PRIMARY KEY,
                last_action_id INT
            )
            """)
            cur.execute("INSERT INTO cdc_offset(id,last_action_id) VALUES (1,0) ON CONFLICT (id) DO NOTHING")

            # Load offset
            self.load_offset(cur)

            # Fetch CDC rows
            cur.execute("""
            SELECT action_id, emp_id, emp_FN, emp_LN, emp_dob, emp_city, emp_salary, action
            FROM emp_cdc
            WHERE action_id > %s
            ORDER BY action_id ASC
            """, (self.last_action_id,))
            records = cur.fetchall()

            if records:
                print(f'Fetched {len(records)} new CDC records.')
                # Produce each to Kafka
                for r in records:
                    emp = Employee.from_line(r)
                    self.produce(employee_topic_name, value=emp.to_json())
                    self.flush()
                    self.last_action_id = r[0]

                # Save last offset
                self.save_offset(cur, self.last_action_id)

            cur.close()
            conn.close()
        except Exception as err:
            print(f'Error fetching CDC data: {err}')
        
        return


class CSVLoader:
    """Reads employees.csv and produces Employee objects."""
    def __init__(self, csv_file):
        self.csv_file = csv_file

    def read_csv(self):
        with open(self.csv_file, newline='', encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                yield Employee(
                    action_id=0,
                    emp_id=0,
                    emp_FN=row['First Name'],
                    emp_LN=row['Last Name'],
                    emp_dob=row['Date of Birth'],
                    emp_city=row['City'],
                    emp_salary=int(row['Salary']),
                    action='insert'
                )


if __name__ == '__main__':
    encoder = StringSerializer('utf-8')
    producer = cdcProducer()

    # --- Step 0: Load CSV snapshot via Employee objects ---
    loader = CSVLoader(csv_file)
    for emp in loader.read_csv():
        try:
            # Insert into employees table (source DB)
            conn = psycopg2.connect(
                host="localhost",
                database="postgres",
                user="postgres",
                port='5432',
                password="postgres"
            )
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO employees(first_name, last_name, dob, city, salary)
                VALUES (%s,%s,%s,%s,%s)
            """, (emp.emp_FN, emp.emp_LN, emp.emp_dob, emp.emp_city, emp.emp_salary))
            cur.close()
            conn.close()
        except Exception as e:
            print(f"Error inserting CSV row: {e}")

    print("CSV snapshot loaded into source DB!")

    while producer.running:
        producer.fetch_cdc()
        time.sleep(1)