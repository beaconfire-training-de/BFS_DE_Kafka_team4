# Testing Guide - Step by Step

This guide walks you through testing the CDC pipeline functionality from scratch, starting with Docker initialization.

## Prerequisites Check

Before starting, ensure you have:
- Docker Desktop installed and running
- Docker Compose installed
- Python 3.8+ installed
- Required Python packages installed

## Step 1: Initialize Docker Services

### 1.1 Start Docker Infrastructure

Open a terminal in the project root directory and run:

```bash
docker-compose up -d
```

This command starts:
- **Zookeeper** (port 22181)
- **Kafka** (port 29092)
- **Source PostgreSQL** (port 5432)
- **Destination PostgreSQL** (port 5433)
- **Kafka Setup** (creates topics automatically)

### 1.2 Wait for Services to Initialize

Wait 10-15 seconds for all services to start up. You can monitor the logs:

```bash
# View all logs
docker-compose logs -f

# Or check specific services
docker-compose logs kafka-setup
docker-compose logs db_source
docker-compose logs db_dst
```

### 1.3 Verify Services are Running

```bash
docker-compose ps
```

Expected output should show all services as "Up":
- zookeeper
- kafka
- db_source
- db_dst
- kafka-setup (may show as "Exited" - this is normal, it runs once)

### 1.4 Verify Kafka Topics Were Created

```bash
# Get Kafka container name/ID
$KAFKA_CONTAINER = docker-compose ps -q kafka

# List topics
docker exec $KAFKA_CONTAINER kafka-topics --list --bootstrap-server kafka:9092
```

Expected topics:
- `__consumer_offsets` (internal Kafka topic)
- `bf_employee_cdc` (main CDC topic)
- `bf_employee_cdc_dlq` (dead letter queue topic)

If topics are missing, check kafka-setup logs:
```bash
docker-compose logs kafka-setup
```

## Step 2: Install Python Dependencies

### 2.1 Install Required Packages

```bash
pip install -r requirements.txt
```

Or install manually:
```bash
pip install confluent-kafka psycopg2-binary pandas
```

### 2.2 Verify Installation

```bash
python -c "import confluent_kafka; import psycopg2; print('Dependencies OK')"
```

## Step 3: Verify Database Setup

### 3.1 Check Source Database

```bash
# Connect to source database
psql -h localhost -p 5432 -U postgres -d postgres
# Password: postgres
```

Inside psql, verify tables exist:
```sql
\dt
```

Expected tables:
- `employees` (main table)
- `emp_cdc` (CDC change log)
- `cdc_offset` (offset tracking)

Check trigger exists:
```sql
SELECT tgname, tgrelid::regclass, tgenabled 
FROM pg_trigger 
WHERE tgname = 'employee_audit_trigger';
```

Check initial offset:
```sql
SELECT * FROM cdc_offset;
-- Should show: id=1, last_action_id=0
```

Exit psql: `\q`

### 3.2 Check Destination Database

```bash
# Connect to destination database
psql -h localhost -p 5433 -U postgres -d postgres
# Password: postgres
```

Inside psql:
```sql
\dt
```

Expected table:
- `employees` (should be empty initially)

```sql
SELECT COUNT(*) FROM employees;
-- Should return: 0
```

Exit psql: `\q`

## Step 4: Start Producer

### 4.1 Start Producer in Terminal 1

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

**Keep this terminal open** - the producer will continue polling for changes.

## Step 5: Start Consumer

### 5.1 Start Consumer in Terminal 2

Open a **new terminal** and run:

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
Waiting for messages...
```

**Keep this terminal open** - the consumer will wait for messages.

## Step 6: Manual Testing

### 6.1 Test INSERT Operation

Open a **third terminal** and connect to source database:

```bash
psql -h localhost -p 5432 -U postgres -d postgres
```

Insert a test record:
```sql
INSERT INTO employees (emp_id, first_name, last_name, dob, city, salary)
VALUES (100, 'John', 'Doe', '1990-01-01', 'New York', 75000);
```

**Watch Terminal 1 (Producer):**
- Should show: `Fetched 1 new CDC records.`
- Should show: `Published CDC record: action_id=X, emp_id=100, action=INSERT`
- Should show: `Successfully published 1 records. Offset saved: X`

**Watch Terminal 2 (Consumer):**
- Should show: `Consuming: action_id=X, emp_id=100, action=INSERT`
- Should show: `INSERT operation completed for emp_id=100`
- Should show: `Successfully processed message: emp_id=100, action=INSERT`

**Verify in Destination Database:**
```bash
psql -h localhost -p 5433 -U postgres -d postgres
```

```sql
SELECT * FROM employees WHERE emp_id = 100;
```

Expected output:
```
 emp_id | first_name | last_name |    dob     |   city    | salary
--------+------------+-----------+------------+-----------+--------
    100 | John       | Doe       | 1990-01-01 | New York  |  75000
```

### 6.2 Test UPDATE Operation

In source database (Terminal 3):
```sql
UPDATE employees 
SET salary = 80000, city = 'Boston' 
WHERE emp_id = 100;
```

**Watch Producer and Consumer logs** - should show UPDATE operations.

**Verify in Destination Database:**
```sql
SELECT emp_id, first_name, salary, city FROM employees WHERE emp_id = 100;
```

Expected:
```
 emp_id | first_name | salary |  city
--------+------------+--------+--------
    100 | John       |  80000 | Boston
```

### 6.3 Test DELETE Operation

In source database (Terminal 3):
```sql
DELETE FROM employees WHERE emp_id = 100;
```

**Watch Producer and Consumer logs** - should show DELETE operations.

**Verify in Destination Database:**
```sql
SELECT COUNT(*) FROM employees WHERE emp_id = 100;
```

Expected: `0` (record should be deleted)

## Step 7: Automated Testing

### 7.1 Run Automated Test Suite

With **producer.py** and **consumer.py** still running, open a **fourth terminal**:

```bash
python test_pipeline.py
```

This script will:
1. Test database connections
2. Test CDC trigger functionality
3. Test INSERT replication
4. Test UPDATE replication
5. Test DELETE replication

**Expected Output:**
```
============================================================
CDC Pipeline Verification Tests
============================================================

NOTE: Make sure producer.py and consumer.py are running before running these tests!
Press Ctrl+C to cancel, or wait 5 seconds to continue...

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

============================================================
TEST 4: UPDATE Operation Replication
============================================================
✓ Record updated in source database (emp_id=8888)
  Waiting 10 seconds for replication...
✓ UPDATE replication successful

============================================================
TEST 5: DELETE Operation Replication
============================================================
✓ Record deleted in source database (emp_id=8888)
  Waiting 10 seconds for replication...
✓ DELETE replication successful - record removed from destination

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

## Step 8: Test DLQ (Dead Letter Queue)

### 8.1 Test Consumer DLQ

**Stop destination database temporarily:**
```bash
docker-compose stop db_dst
```

**Insert a record in source database:**
```sql
INSERT INTO employees (emp_id, first_name, last_name, dob, city, salary)
VALUES (200, 'Test', 'DLQ', '1990-01-01', 'Test City', 50000);
```

**Watch Consumer logs** - should show retry attempts:
```
Error processing message (attempt 1/4): ...
Error processing message (attempt 2/4): ...
Error processing message (attempt 3/4): ...
Max retries exceeded. Sending to DLQ.
Message sent to DLQ: bf_employee_cdc_dlq
```

**Check DLQ messages:**
```bash
$KAFKA_CONTAINER = docker-compose ps -q kafka
docker exec $KAFKA_CONTAINER kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic bf_employee_cdc_dlq \
  --from-beginning \
  --max-messages 1
```

**Restart destination database:**
```bash
docker-compose start db_dst
```

## Step 9: Verify Offset Tracking

### 9.1 Test Offset Persistence

1. **Stop Producer** (Ctrl+C in Terminal 1)
2. **Insert a new record** in source database
3. **Restart Producer:**
   ```bash
   python producer.py
   ```
4. **Verify** - Producer should show: `Loaded offset: X` (where X is the last processed action_id)
5. **Verify** - Producer should NOT republish old messages, only process new ones

## Step 10: Clean Up

### 10.1 Stop All Services

```bash
# Stop producer and consumer (Ctrl+C in their terminals)

# Stop Docker services
docker-compose down
```

### 10.2 Remove Volumes (Optional - removes all data)

```bash
docker-compose down -v
```

## Troubleshooting

### Services Won't Start

```bash
# Check Docker is running
docker ps

# Check for port conflicts
netstat -an | findstr "5432 5433 29092 22181"

# View detailed logs
docker-compose logs
```

### Topics Not Created

```bash
# Manually create topics
$KAFKA_CONTAINER = docker-compose ps -q kafka

docker exec $KAFKA_CONTAINER kafka-topics --create \
  --bootstrap-server kafka:9092 \
  --partitions 3 \
  --replication-factor 1 \
  --topic bf_employee_cdc

docker exec $KAFKA_CONTAINER kafka-topics --create \
  --bootstrap-server kafka:9092 \
  --partitions 1 \
  --replication-factor 1 \
  --topic bf_employee_cdc_dlq
```

### Database Connection Issues

```bash
# Verify databases are accessible
psql -h localhost -p 5432 -U postgres -d postgres -c "SELECT 1"
psql -h localhost -p 5433 -U postgres -d postgres -c "SELECT 1"
```

### Producer/Consumer Not Processing

1. Check both are running
2. Check Kafka is accessible: `docker-compose logs kafka`
3. Verify topics exist (Step 1.4)
4. Check for errors in producer/consumer logs

## Quick Reference

| Service | Port | Connection String |
|---------|------|-------------------|
| Source DB | 5432 | `psql -h localhost -p 5432 -U postgres -d postgres` |
| Destination DB | 5433 | `psql -h localhost -p 5433 -U postgres -d postgres` |
| Kafka | 29092 | `localhost:29092` |
| Zookeeper | 22181 | Internal only |

| Command | Purpose |
|---------|---------|
| `docker-compose up -d` | Start all services |
| `docker-compose ps` | Check service status |
| `docker-compose logs` | View all logs |
| `docker-compose down` | Stop all services |
| `python producer.py` | Start CDC producer |
| `python consumer.py` | Start CDC consumer |
| `python test_pipeline.py` | Run automated tests |
