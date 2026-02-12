# BFS_DE_Kafka_team4 - Change Data Capture (CDC) Pipeline

A Change Data Capture (CDC) system using Apache Kafka to replicate employee data changes between two PostgreSQL databases in real-time.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Setup](#setup)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [DLQ (Dead Letter Queue)](#dlq-dead-letter-queue)
- [Verification & Testing](#verification--testing)
- [Troubleshooting](#troubleshooting)

## Overview

This project implements a CDC pipeline that:
1. Captures changes (INSERT, UPDATE, DELETE) from a source PostgreSQL database
2. Publishes change events to Apache Kafka
3. Consumes events from Kafka and applies them to a destination PostgreSQL database
4. Handles failures gracefully using a Dead Letter Queue (DLQ)

## Architecture

```
┌─────────────────┐         ┌──────────────┐         ┌─────────────────┐
│  Source DB      │         │    Kafka     │         │ Destination DB   │
│  (Port 5432)    │         │  (Port 29092)│         │  (Port 5433)     │
│                 │         │              │         │                  │
│  employees      │         │  bf_employee │         │  employees       │
│  emp_cdc        │────────▶│  _cdc        │────────▶│                  │
│  (triggers)     │         │              │         │                  │
└─────────────────┘         └──────────────┘         └─────────────────┘
                                     │
                                     │ Failed Messages
                                     ▼
                            ┌─────────────────┐
                            │       DLQ       │
                            │  bf_employee_   │
                            │  cdc_dlq        │
                            └─────────────────┘
```

### Components

1. **Source Database (`db_source`)**: 
   - Contains `employees` table with PostgreSQL triggers
   - `emp_cdc` table captures all changes
   - `cdc_offset` table tracks processing progress

2. **Kafka Cluster**:
   - **Main Topic**: `bf_employee_cdc` (3 partitions)
   - **DLQ Topic**: `bf_employee_cdc_dlq` (1 partition)

3. **Destination Database (`db_dst`)**:
   - Contains `employees` table that mirrors the source

4. **Producer (`producer.py`)**:
   - Polls source database for new CDC records
   - Publishes changes to Kafka
   - Tracks offset to resume after restarts
   - Sends failed messages to DLQ

5. **Consumer (`consumer.py`)**:
   - Consumes messages from Kafka
   - Applies changes to destination database
   - Retries failed messages (up to 3 times)
   - Sends permanently failed messages to DLQ

## Features

-  **Real-time CDC**: Captures and replicates database changes in near real-time
-  **Offset Tracking**: Resumes processing from last successful position after restarts
-  **Dead Letter Queue**: Failed messages are sent to DLQ for manual inspection/reprocessing
-  **Retry Logic**: Automatic retries with exponential backoff
-  **Error Handling**: Comprehensive error handling and logging
-  **Docker Compose**: Easy setup with containerized services
-  **Partitioning**: Kafka topics partitioned for scalability

##  Prerequisites

- Docker and Docker Compose
- Python 3.8+
- Required Python packages (install via `pip install -r requirements.txt`):
  - `confluent-kafka`
  - `psycopg2-binary`
  - `pandas` (optional, for data analysis)

##  Setup

### 1. Clone the Repository

```bash
git clone <repository-url>
cd BFS_DE_Kafka_team4
```

### 2. Start Infrastructure Services

Start Kafka, Zookeeper, and PostgreSQL databases:

```bash
docker-compose up -d
```

This will start:
- **Zookeeper** (port 22181)
- **Kafka** (port 29092)
- **Source PostgreSQL** (port 5432)
- **Destination PostgreSQL** (port 5433)
- **Kafka Setup** (creates topics automatically)

Wait a few seconds for services to initialize. Verify topics were created:

```bash
docker-compose logs kafka-setup
```

### 3. Verify Topics

You can verify topics exist using the admin client:

```bash
python admin.py
```

Or manually check:

```bash
docker exec -it <kafka-container-id> kafka-topics --list --bootstrap-server kafka:9092
```

### 4. Install Python Dependencies

```bash
pip install confluent-kafka psycopg2-binary pandas
```

Or create a `requirements.txt`:

```txt
confluent-kafka>=2.3.0
psycopg2-binary>=2.9.0
pandas>=2.0.0
```

Then install:

```bash
pip install -r requirements.txt
```

##  Usage

### Starting the Producer

The producer polls the source database and publishes CDC records to Kafka:

```bash
python producer.py
```

**What it does:**
- Connects to source database (localhost:5432)
- Loads last processed offset from `cdc_offset` table
- Polls `emp_cdc` table for new records
- Publishes records to `bf_employee_cdc` topic
- Saves offset after successful publishing
- Sends failed messages to DLQ

**Output example:**
```
CDC Producer started. Polling for new records...
Kafka broker: localhost:29092
Topic: bf_employee_cdc
DLQ Topic: bf_employee_cdc_dlq
Loaded offset: 0
Fetched 5 new CDC records.
Published CDC record: action_id=1, emp_id=1, action=INSERT
Successfully published 5 records. Offset saved: 5
```

### Starting the Consumer

The consumer processes messages from Kafka and applies them to the destination database:

```bash
python consumer.py
```

**What it does:**
- Subscribes to `bf_employee_cdc` topic
- Consumes messages from Kafka
- Applies INSERT/UPDATE/DELETE operations to destination database
- Retries failed messages (up to 3 times)
- Sends permanently failed messages to DLQ

**Output example:**
```
CDC Consumer started.
Kafka broker: localhost:29092
Consumer group: bf_group
Topic: bf_employee_cdc
DLQ Topic: bf_employee_cdc_dlq
Subscribed to topics: ['bf_employee_cdc']
Consuming: action_id=1, emp_id=1, action=INSERT
INSERT operation completed for emp_id=1
Successfully processed message: emp_id=1, action=INSERT
```

### Testing the Pipeline

1. **Insert test data into source database:**

```sql
-- Connect to source database (port 5432)
psql -h localhost -p 5432 -U postgres -d postgres

-- Insert an employee (trigger will create CDC record)
INSERT INTO employees (emp_id, first_name, last_name, dob, city, salary)
VALUES (1, 'John', 'Doe', '1990-01-01', 'New York', 75000);
```

2. **Watch the producer** - it should detect and publish the change

3. **Watch the consumer** - it should consume and apply the change to destination DB

4. **Verify in destination database:**

```sql
-- Connect to destination database (port 5433)
psql -h localhost -p 5433 -U postgres -d postgres

-- Check if employee was replicated
SELECT * FROM employees WHERE emp_id = 1;
```

### Testing DLQ

To test DLQ functionality, you can:

1. **Stop the destination database** temporarily:
```bash
docker-compose stop db_dst
```

2. **Run the consumer** - it will fail to connect and send messages to DLQ after retries

3. **Check DLQ messages:**
```bash
# Consume from DLQ topic
docker exec -it <kafka-container-id> kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic bf_employee_cdc_dlq \
  --from-beginning
```

## 📁 Project Structure

```
BFS_DE_Kafka_team4/
│
├── producer.py              # CDC Producer - reads from source DB, publishes to Kafka
├── consumer.py              # CDC Consumer - consumes from Kafka, writes to dest DB
├── admin.py                 # Kafka Admin Client - topic management utilities
├── employee.py              # Employee data model class
├── employees.csv            # Sample employee data (optional)
├── docker-compose.yml       # Docker Compose configuration
├── README.md                # This file
│
└── scripts/
    ├── source/
    │   └── init.sql         # Source DB schema and triggers
    └── destination/
        └── init.sql         # Destination DB schema
```

### Key Files Explained

- **`producer.py`**: Implements `cdcProducer` class that:
  - Fetches CDC records from source database
  - Tracks offset to resume processing
  - Publishes to Kafka with error handling
  - Sends failed messages to DLQ

- **`consumer.py`**: Implements `cdcConsumer` class that:
  - Consumes messages from Kafka
  - Applies changes to destination database
  - Implements retry logic with exponential backoff
  - Sends failed messages to DLQ

- **`admin.py`**: Utility for managing Kafka topics:
  - Create/delete topics
  - Check topic existence
  - Inspect consumer groups

- **`employee.py`**: Data model for employee records with serialization

##  DLQ (Dead Letter Queue)

The Dead Letter Queue (`bf_employee_cdc_dlq`) is used to handle messages that cannot be processed successfully.

### When Messages Go to DLQ

**Producer DLQ:**
- Message delivery failures (network issues, broker unavailable)
- Serialization errors

**Consumer DLQ:**
- Database connection failures (after 3 retries)
- Invalid message format (JSON parsing errors)
- Database constraint violations (after 3 retries)
- Unknown action types

### DLQ Message Format

DLQ messages contain:
```json
{
  "original_topic": "bf_employee_cdc",
  "original_key": "1",
  "original_value": "{...employee data...}",
  "error": "Error message",
  "partition": 0,
  "offset": 123,
  "timestamp": 1234567890.123
}
```

### Processing DLQ Messages

To reprocess DLQ messages:

1. **Consume from DLQ:**
```bash
docker exec -it <kafka-container-id> kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic bf_employee_cdc_dlq \
  --from-beginning
```

2. **Fix the issue** (e.g., database connection, data format)

3. **Republish to main topic** or process manually

## Verification & Testing

This section provides step-by-step instructions to verify that all components of the CDC pipeline are working correctly.

### Step 1: Verify Infrastructure is Running

First, ensure all Docker services are up and running:

```bash
# Check all services status
docker-compose ps

# Expected output should show all services as "Up":
# - zookeeper
# - kafka
# - db_source
# - db_dst
# - kafka-setup (may show as "Exited" - this is normal, it runs once)
```

**Verify Kafka Topics:**
```bash
# Get Kafka container name
KAFKA_CONTAINER=$(docker-compose ps -q kafka)

# List topics
docker exec $KAFKA_CONTAINER kafka-topics --list --bootstrap-server kafka:9092

# Expected output:
# __consumer_offsets
# bf_employee_cdc
# bf_employee_cdc_dlq
```

**Verify Topic Details:**
```bash
# Check main topic details
docker exec $KAFKA_CONTAINER kafka-topics --describe --bootstrap-server kafka:9092 --topic bf_employee_cdc

# Check DLQ topic details
docker exec $KAFKA_CONTAINER kafka-topics --describe --bootstrap-server kafka:9092 --topic bf_employee_cdc_dlq
```

### Step 2: Verify Database Setup

**Check Source Database:**
```bash
# Connect to source database
psql -h localhost -p 5432 -U postgres -d postgres

# Verify tables exist
\dt

# Expected tables:
# - employees
# - emp_cdc
# - cdc_offset

# Check trigger exists
SELECT tgname, tgrelid::regclass, tgenabled 
FROM pg_trigger 
WHERE tgname = 'employee_audit_trigger';

# Check offset table
SELECT * FROM cdc_offset;
# Should show: id=1, last_action_id=0 (or current offset)
```

**Check Destination Database:**
```bash
# Connect to destination database
psql -h localhost -p 5433 -U postgres -d postgres

# Verify table exists
\dt

# Expected table:
# - employees

# Check if table is empty (should be initially)
SELECT COUNT(*) FROM employees;
```

### Step 3: Test Producer Component

**Start Producer in Terminal 1:**
```bash
python producer.py
```

**Expected Output:**
```
CDC Producer started. Polling for new records...
Kafka broker: localhost:29092
Topic: bf_employee_cdc
DLQ Topic: bf_employee_cdc_dlq
Loaded offset: 0
No new CDC records found. Waiting 5 seconds...
```

**Insert Test Data in Source Database:**
```bash
# In a new terminal, connect to source DB
psql -h localhost -p 5432 -U postgres -d postgres

# Insert test employee
INSERT INTO employees (emp_id, first_name, last_name, dob, city, salary)
VALUES (100, 'Test', 'User', '1990-01-01', 'Test City', 50000);

# Verify CDC record was created
SELECT * FROM emp_cdc ORDER BY action_id DESC LIMIT 1;
```

**Verify Producer Detected and Published:**
In Terminal 1 (producer), you should see:
```
Fetched 1 new CDC records.
Published CDC record: action_id=1, emp_id=100, action=INSERT
Message delivered to bf_employee_cdc [0] at offset 0
Successfully published 1 records. Offset saved: 1
```

**Verify Message in Kafka:**
```bash
# Consume messages from Kafka topic (should see the message)
docker exec $KAFKA_CONTAINER kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic bf_employee_cdc \
  --from-beginning \
  --max-messages 1
```

### Step 4: Test Consumer Component

**Start Consumer in Terminal 2:**
```bash
python consumer.py
```

**Expected Output:**
```
CDC Consumer started.
Kafka broker: localhost:29092
Consumer group: bf_group
Topic: bf_employee_cdc
DLQ Topic: bf_employee_cdc_dlq
Subscribed to topics: ['bf_employee_cdc']
Consuming: action_id=1, emp_id=100, action=INSERT
Message details: topic=bf_employee_cdc, partition=0, offset=0
INSERT operation completed for emp_id=100
Successfully processed message: emp_id=100, action=INSERT
```

**Verify Data Replicated to Destination:**
```bash
# Connect to destination database
psql -h localhost -p 5433 -U postgres -d postgres

# Check if employee was replicated
SELECT * FROM employees WHERE emp_id = 100;

# Expected output:
# emp_id | first_name | last_name |    dob     |    city     | salary
# -------+------------+-----------+------------+-------------+--------
#    100 | Test       | User      | 1990-01-01 | Test City   |  50000
```

### Step 5: Test Full CDC Pipeline (INSERT, UPDATE, DELETE)

**Test INSERT:**
```sql
-- In source database
INSERT INTO employees (emp_id, first_name, last_name, dob, city, salary)
VALUES (101, 'Alice', 'Smith', '1985-05-15', 'New York', 75000);
```

**Verify:**
- Producer logs show message published
- Consumer logs show INSERT operation
- Destination DB has the record: `SELECT * FROM employees WHERE emp_id = 101;`

**Test UPDATE:**
```sql
-- In source database
UPDATE employees 
SET salary = 80000, city = 'Boston' 
WHERE emp_id = 101;
```

**Verify:**
- Producer logs show UPDATE message published
- Consumer logs show UPDATE operation
- Destination DB reflects changes: `SELECT * FROM employees WHERE emp_id = 101;`
- Should show: salary=80000, city='Boston'

**Test DELETE:**
```sql
-- In source database
DELETE FROM employees WHERE emp_id = 101;
```

**Verify:**
- Producer logs show DELETE message published
- Consumer logs show DELETE operation
- Destination DB record is removed: `SELECT * FROM employees WHERE emp_id = 101;`
- Should return 0 rows

### Step 6: Test Offset Tracking

**Stop Producer (Ctrl+C), then restart:**
```bash
python producer.py
```

**Verify:**
- Producer should load the last offset: `Loaded offset: X` (where X is the last action_id)
- Should NOT republish old messages
- Only processes new CDC records

**Insert new record:**
```sql
INSERT INTO employees (emp_id, first_name, last_name, dob, city, salary)
VALUES (102, 'Bob', 'Jones', '1992-03-20', 'Chicago', 60000);
```

**Verify:**
- Producer only processes the new record (action_id = X+1)
- Old records are not reprocessed

### Step 7: Test DLQ Functionality

**Test Consumer DLQ (Database Failure):**

1. **Stop destination database temporarily:**
```bash
docker-compose stop db_dst
```

2. **Insert a record in source DB:**
```sql
INSERT INTO employees (emp_id, first_name, last_name, dob, city, salary)
VALUES (103, 'Charlie', 'Brown', '1988-07-10', 'Seattle', 70000);
```

3. **Watch consumer logs** - should show retry attempts:
```
Error processing message (attempt 1/4): ...
Error processing message (attempt 2/4): ...
Error processing message (attempt 3/4): ...
Max retries exceeded. Sending to DLQ.
Message sent to DLQ: bf_employee_cdc_dlq
```

4. **Restart destination database:**
```bash
docker-compose start db_dst
```

5. **Check DLQ messages:**
```bash
docker exec $KAFKA_CONTAINER kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic bf_employee_cdc_dlq \
  --from-beginning \
  --max-messages 1
```

**Expected DLQ message format:**
```json
{
  "original_topic": "bf_employee_cdc",
  "original_key": "103",
  "original_value": "{...employee data...}",
  "error": "connection to server at \"localhost\" (::1), port 5433 failed...",
  "partition": 0,
  "offset": 3,
  "timestamp": 1234567890.123
}
```

### Step 8: Verify Consumer Group

**Check consumer group status:**
```bash
docker exec $KAFKA_CONTAINER kafka-consumer-groups \
  --bootstrap-server kafka:9092 \
  --describe \
  --group bf_group
```

**Expected output shows:**
- Consumer group: `bf_group`
- Topic: `bf_employee_cdc`
- Current offset, lag, etc.

### Step 9: End-to-End Integration Test

**Complete Test Scenario:**

1. **Start both producer and consumer** in separate terminals

2. **Insert multiple records:**
```sql
-- In source database
INSERT INTO employees (emp_id, first_name, last_name, dob, city, salary) VALUES
(200, 'David', 'Wilson', '1991-01-01', 'Miami', 65000),
(201, 'Emma', 'Davis', '1993-02-02', 'Denver', 70000),
(202, 'Frank', 'Miller', '1989-03-03', 'Portland', 72000);
```

3. **Verify all records replicated:**
```sql
-- In destination database
SELECT emp_id, first_name, last_name, city, salary 
FROM employees 
WHERE emp_id IN (200, 201, 202)
ORDER BY emp_id;
```

4. **Update one record:**
```sql
UPDATE employees SET salary = 75000 WHERE emp_id = 200;
```

5. **Delete one record:**
```sql
DELETE FROM employees WHERE emp_id = 202;
```

6. **Final verification:**
```sql
-- Destination should have 2 records (200 updated, 201 unchanged, 202 deleted)
SELECT emp_id, first_name, salary FROM employees WHERE emp_id IN (200, 201, 202) ORDER BY emp_id;

-- Expected:
-- emp_id | first_name | salary
-- -------+------------+--------
--    200 | David      |  75000
--    201 | Emma       |  70000
```

### Step 10: Performance Verification

**Check message throughput:**
```bash
# Monitor Kafka topic
docker exec $KAFKA_CONTAINER kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list kafka:9092 \
  --topic bf_employee_cdc \
  --time -1
```

**Check consumer lag:**
```bash
docker exec $KAFKA_CONTAINER kafka-consumer-groups \
  --bootstrap-server kafka:9092 \
  --describe \
  --group bf_group
```

### Automated Testing Script

A comprehensive test script is provided to verify the pipeline:

**Run the automated test suite:**
```bash
# Make sure producer.py and consumer.py are running first!
python test_pipeline.py
```

This script automatically tests:
- Database connections (source and destination)
- CDC trigger functionality
- INSERT operation replication
- UPDATE operation replication
- DELETE operation replication

**Expected output:**
```
============================================================
CDC Pipeline Verification Tests
============================================================

============================================================
TEST 1: Database Connections
============================================================
✓ Source database connection: OK
✓ Destination database connection: OK

============================================================
TEST 2: CDC Trigger Functionality
============================================================
✓ CDC trigger is working - new CDC record created

============================================================
TEST 3: INSERT Operation Replication
============================================================
✓ Record inserted in source database (emp_id=8888)
  Waiting 10 seconds for replication...
✓ INSERT replication successful - record found in destination

... (more tests)

============================================================
TEST SUMMARY
============================================================
✓ PASS: Database Connections
✓ PASS: CDC Trigger
✓ PASS: INSERT Replication
✓ PASS: UPDATE Replication
✓ PASS: DELETE Replication

============================================================
Results: 5/5 tests passed
============================================================

🎉 All tests passed! Your CDC pipeline is working correctly.
```

### Success Criteria

Your pipeline is working correctly if:

✅ All Docker services are running  
✅ Kafka topics exist (`bf_employee_cdc` and `bf_employee_cdc_dlq`)  
✅ Producer detects and publishes CDC records  
✅ Consumer processes messages and applies to destination DB  
✅ INSERT, UPDATE, DELETE operations are replicated correctly  
✅ Offset tracking works (producer resumes from last position)  
✅ DLQ captures failed messages  
✅ No data loss or duplication  
✅ Consumer group shows proper offset tracking  

## Troubleshooting

### Producer Issues

**Problem**: Producer can't connect to Kafka
- **Solution**: Check if Kafka is running: `docker-compose ps`
- Verify Kafka port: `localhost:29092` (outside Docker) or `kafka:9092` (inside Docker)

**Problem**: No new records detected
- **Solution**: Check if CDC records exist: `SELECT * FROM emp_cdc WHERE action_id > 0;`
- Verify offset table: `SELECT * FROM cdc_offset;`

**Problem**: Messages not being published
- **Solution**: Check producer logs for errors
- Verify topic exists: `python admin.py`
- Check Kafka logs: `docker-compose logs kafka`

### Consumer Issues

**Problem**: Consumer can't connect to Kafka
- **Solution**: Same as producer - verify Kafka is running and accessible

**Problem**: Consumer can't connect to destination database
- **Solution**: Check if `db_dst` is running: `docker-compose ps db_dst`
- Verify port: `localhost:5433`
- Check database logs: `docker-compose logs db_dst`

**Problem**: Messages going to DLQ
- **Solution**: Check DLQ messages for error details
- Verify destination database schema matches source
- Check for constraint violations or data type mismatches

### Database Issues

**Problem**: Triggers not creating CDC records
- **Solution**: Verify trigger exists:
```sql
SELECT * FROM pg_trigger WHERE tgname = 'employee_audit_trigger';
```
- Check trigger function: `\df log_employee_changes`

**Problem**: Offset not saving
- **Solution**: Verify `cdc_offset` table exists and has row with `id=1`
- Check database connection and autocommit settings

### General Issues

**Problem**: Topics not created
- **Solution**: Check `kafka-setup` container logs: `docker-compose logs kafka-setup`
- Manually create topics using `admin.py` or docker exec

**Problem**: Port conflicts
- **Solution**: Modify ports in `docker-compose.yml` if conflicts occur
- Update connection strings in producer/consumer accordingly

## 📝 Notes

- **Offset Tracking**: The producer saves offset only after successfully publishing all records in a batch
- **Retry Logic**: Consumer retries failed messages up to 3 times with exponential backoff (2s, 4s, 8s)
- **Partitioning**: Employee ID is used as message key to ensure same employee records go to same partition
- **Auto-commit**: Consumer uses auto-commit for simplicity; consider manual commits for production
- **DLQ Producer**: Separate producer instances are used for DLQ to avoid circular dependencies

## 🔐 Security Considerations

⚠️ **For Production Use:**

- Use environment variables for database credentials
- Enable SSL/TLS for Kafka connections
- Use SASL authentication for Kafka
- Implement proper secret management
- Use connection pooling for database connections
- Add monitoring and alerting
- Consider using Schema Registry for message validation

## 📚 Additional Resources

- [Confluent Kafka Python Client Documentation](https://docs.confluent.io/kafka-clients/python/current/overview.html)
- [PostgreSQL Triggers Documentation](https://www.postgresql.org/docs/current/triggers.html)
- [Kafka Best Practices](https://kafka.apache.org/documentation/#bestpractices)

## 👥 Team

BFS_DE_Kafka_team4 - BeaconFire Staffing Solutions

## 📄 License

See individual file headers for license information
