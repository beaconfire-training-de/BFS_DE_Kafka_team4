# DLQ (Dead Letter Queue) Testing Guide

This guide explains how to test the Dead Letter Queue (DLQ) functionality in the CDC pipeline.

## Overview

The DLQ (`bf_employee_cdc_dlq`) is used to handle messages that cannot be processed successfully. Messages are sent to DLQ in two scenarios:

1. **Producer DLQ**: When message delivery to Kafka fails
2. **Consumer DLQ**: When message processing fails after retries

## Prerequisites

Before testing DLQ, ensure:
- Docker services are running (`docker-compose ps`)
- Producer is running (`python producer.py`)
- Consumer is running (`python consumer.py`)
- Kafka topics exist (including `bf_employee_cdc_dlq`)

## Test 1: Verify DLQ Topic Exists

### Step 1.1: Check Topics

```bash
# Get Kafka container ID
$KAFKA_CONTAINER = docker-compose ps -q kafka

# List all topics
docker exec $KAFKA_CONTAINER kafka-topics --list --bootstrap-server kafka:9092
```

**Expected output should include:**
- `bf_employee_cdc` (main topic)
- `bf_employee_cdc_dlq` (DLQ topic)

### Step 1.2: Verify DLQ Topic Details

```bash
docker exec $KAFKA_CONTAINER kafka-topics --describe \
  --bootstrap-server kafka:9092 \
  --topic bf_employee_cdc_dlq
```

**Expected output:**
```
Topic: bf_employee_cdc_dlq	PartitionCount: 1	ReplicationFactor: 1
```

## Test 2: Consumer DLQ - Database Failure

This test simulates a scenario where the destination database is unavailable, causing the consumer to retry and eventually send messages to DLQ.

### Step 2.1: Stop Destination Database

```bash
docker-compose stop db_dst
```

### Step 2.2: Insert Test Record in Source Database

```bash
psql -h localhost -p 5432 -U postgres -d postgres
```

```sql
INSERT INTO employees (emp_id, first_name, last_name, dob, city, salary)
VALUES (5000, 'DLQ', 'Test', '1990-01-01', 'Test City', 50000);
```

Exit psql: `\q`

### Step 2.3: Watch Consumer Logs

In the terminal where `consumer.py` is running, you should see:

```
Consuming: action_id=X, emp_id=5000, action=INSERT
Database error (attempt 1/4): connection to server at "localhost" (::1), port 5433 failed...
Database error (attempt 2/4): connection to server at "localhost" (::1), port 5433 failed...
Database error (attempt 3/4): connection to server at "localhost" (::1), port 5433 failed...
Max retries exceeded. Sending to DLQ.
Message sent to DLQ: bf_employee_cdc_dlq
DLQ message sent: success
```

### Step 2.4: Verify DLQ Message

```bash
$KAFKA_CONTAINER = docker-compose ps -q kafka

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
  "original_key": "5000",
  "original_value": "{\"action_id\":X,\"emp_id\":5000,\"emp_FN\":\"DLQ\",\"emp_LN\":\"Test\",\"emp_dob\":\"1990-01-01\",\"emp_city\":\"Test City\",\"emp_salary\":50000,\"action\":\"INSERT\"}",
  "error": "connection to server at \"localhost\" (::1), port 5433 failed...",
  "partition": 0,
  "offset": X,
  "timestamp": 1234567890.123
}
```

### Step 2.5: Restart Destination Database

```bash
docker-compose start db_dst
```

Wait a few seconds for the database to start.

### Step 2.6: Verify Message is in DLQ (Not Reprocessed)

The message should remain in DLQ. The consumer will not automatically reprocess DLQ messages - they need to be manually handled.

## Test 3: Consumer DLQ - Invalid JSON Message

This test verifies that invalid JSON messages are sent to DLQ immediately (without retries).

### Step 3.1: Publish Invalid JSON to Kafka Topic

You'll need to use Kafka console producer or write a small script. Here's a Python script approach:

**Create `test_invalid_json.py`:**
```python
from confluent_kafka import Producer
import json

producer = Producer({'bootstrap.servers': 'localhost:29092'})

# Publish invalid JSON
producer.produce(
    'bf_employee_cdc',
    key='9999',
    value=b'This is not valid JSON {invalid}',
    callback=lambda err, msg: print(f'Published: {err if err else "success"}')
)
producer.flush()
print("Invalid JSON message published")
```

Run it:
```bash
python test_invalid_json.py
```

### Step 3.2: Watch Consumer Logs

The consumer should immediately send the message to DLQ:

```
JSON decode error: Expecting value: line 1 column 1 (char 0)
Message sent to DLQ: bf_employee_cdc_dlq
```

### Step 3.3: Verify DLQ Message

```bash
docker exec $KAFKA_CONTAINER kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic bf_employee_cdc_dlq \
  --from-beginning \
  --max-messages 1
```

The DLQ message should show the JSON decode error.

## Test 4: Consumer DLQ - Unknown Action Type

This test verifies that messages with unknown action types are sent to DLQ.

### Step 4.1: Publish Message with Unknown Action

**Create `test_unknown_action.py`:**
```python
from confluent_kafka import Producer
import json

producer = Producer({'bootstrap.servers': 'localhost:29092'})

# Publish message with unknown action
invalid_message = {
    "action_id": 9999,
    "emp_id": 9999,
    "emp_FN": "Test",
    "emp_LN": "User",
    "emp_dob": "1990-01-01",
    "emp_city": "Test City",
    "emp_salary": 50000,
    "action": "UNKNOWN_ACTION"  # Invalid action type
}

producer.produce(
    'bf_employee_cdc',
    key='9999',
    value=json.dumps(invalid_message).encode('utf-8'),
    callback=lambda err, msg: print(f'Published: {err if err else "success"}')
)
producer.flush()
print("Message with unknown action published")
```

Run it:
```bash
python test_unknown_action.py
```

### Step 4.2: Watch Consumer Logs

The consumer should retry and eventually send to DLQ:

```
Error processing message (attempt 1/4): Unknown action type: UNKNOWN_ACTION
Error processing message (attempt 2/4): Unknown action type: UNKNOWN_ACTION
Error processing message (attempt 3/4): Unknown action type: UNKNOWN_ACTION
Max retries exceeded. Sending to DLQ.
```

## Test 5: Producer DLQ - Kafka Broker Failure

This test verifies that producer sends messages to DLQ when Kafka broker is unavailable.

### Step 5.1: Stop Kafka

```bash
docker-compose stop kafka
```

### Step 5.2: Insert Record in Source Database

```sql
INSERT INTO employees (emp_id, first_name, last_name, dob, city, salary)
VALUES (6000, 'Producer', 'DLQ', '1990-01-01', 'Test City', 50000);
```

### Step 5.3: Watch Producer Logs

The producer should show delivery failures and send to DLQ:

```
Published CDC record: action_id=X, emp_id=6000, action=INSERT
Message delivery failed: [Error 5] Local: Broker transport failure
Message sent to DLQ: bf_employee_cdc_dlq
```

### Step 5.4: Restart Kafka

```bash
docker-compose start kafka
```

Wait for Kafka to fully start (about 10-15 seconds).

### Step 5.5: Verify DLQ Message

```bash
docker exec $KAFKA_CONTAINER kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic bf_employee_cdc_dlq \
  --from-beginning \
  --max-messages 1
```

## Test 6: Automated DLQ Testing

Run the automated DLQ tests:

```bash
python test_pipeline.py
```

This will run:
- DLQ Topic Existence test
- DLQ Message Format verification
- Consumer DLQ manual test (with prompts)

## Monitoring DLQ Messages

### View All DLQ Messages

```bash
$KAFKA_CONTAINER = docker-compose ps -q kafka

docker exec $KAFKA_CONTAINER kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic bf_employee_cdc_dlq \
  --from-beginning
```

### Count DLQ Messages

```bash
docker exec $KAFKA_CONTAINER kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list kafka:9092 \
  --topic bf_employee_cdc_dlq \
  --time -1
```

### View DLQ Message Details (Pretty Print)

```bash
docker exec $KAFKA_CONTAINER kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic bf_employee_cdc_dlq \
  --from-beginning \
  --max-messages 1 | python -m json.tool
```

## DLQ Message Structure

All DLQ messages follow this structure:

```json
{
  "original_topic": "bf_employee_cdc",
  "original_key": "employee_id",
  "original_value": "{...employee JSON data...}",
  "error": "Error message describing why it failed",
  "partition": 0,
  "offset": 123,
  "timestamp": 1234567890.123
}
```

### Field Descriptions

- **original_topic**: The original Kafka topic where the message was published
- **original_key**: The message key (usually employee ID)
- **original_value**: The original message payload (JSON string)
- **error**: Error message explaining why processing failed
- **partition**: Kafka partition number (for consumer DLQ messages)
- **offset**: Kafka offset (for consumer DLQ messages)
- **timestamp**: Unix timestamp when the message was sent to DLQ

## Reprocessing DLQ Messages

DLQ messages need to be manually reprocessed. Here's how:

### Option 1: Manual Reprocessing Script

Create `reprocess_dlq.py`:

```python
from confluent_kafka import Consumer, Producer
import json

# Consumer to read from DLQ
dlq_consumer = Consumer({
    'bootstrap.servers': 'localhost:29092',
    'group.id': 'dlq_reprocessor',
    'auto.offset.reset': 'earliest'
})

# Producer to republish to main topic
producer = Producer({'bootstrap.servers': 'localhost:29092'})

dlq_consumer.subscribe(['bf_employee_cdc_dlq'])

print("Reading DLQ messages...")
try:
    while True:
        msg = dlq_consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            continue
        
        # Parse DLQ message
        dlq_data = json.loads(msg.value().decode('utf-8'))
        
        # Extract original message
        original_value = dlq_data.get('original_value')
        original_key = dlq_data.get('original_key')
        
        if original_value and original_key:
            # Republish to main topic
            producer.produce(
                'bf_employee_cdc',
                key=original_key.encode('utf-8'),
                value=original_value.encode('utf-8')
            )
            producer.flush()
            print(f"Reprocessed message with key: {original_key}")
        
except KeyboardInterrupt:
    print("\nStopped reprocessing")
finally:
    dlq_consumer.close()
```

### Option 2: Manual Extraction and Republish

1. **Extract DLQ message:**
```bash
docker exec $KAFKA_CONTAINER kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic bf_employee_cdc_dlq \
  --from-beginning \
  --max-messages 1 > dlq_message.json
```

2. **Fix the issue** (e.g., database connection, data format)

3. **Republish manually** using Kafka console producer or the reprocessing script

## Troubleshooting

### DLQ Topic Not Created

If the DLQ topic doesn't exist, create it manually:

```bash
$KAFKA_CONTAINER = docker-compose ps -q kafka

docker exec $KAFKA_CONTAINER kafka-topics --create \
  --bootstrap-server kafka:9092 \
  --partitions 1 \
  --replication-factor 1 \
  --topic bf_employee_cdc_dlq
```

### No Messages in DLQ

If no messages appear in DLQ:
1. Verify consumer/producer are running
2. Check logs for error messages
3. Ensure retries are being attempted (check consumer logs)
4. Verify DLQ producer is initialized (check logs for "Message sent to DLQ")

### DLQ Messages Not Formatted Correctly

If DLQ messages are malformed:
1. Check producer/consumer code for DLQ message creation
2. Verify JSON serialization is working
3. Check Kafka message encoding (should be UTF-8)

## Best Practices

1. **Monitor DLQ regularly**: Set up monitoring to alert when messages appear in DLQ
2. **Investigate root causes**: Don't just reprocess - fix the underlying issue
3. **Set retention policy**: Configure DLQ topic retention to prevent unlimited growth
4. **Document common errors**: Keep a log of common DLQ errors and their resolutions
5. **Test DLQ regularly**: Include DLQ testing in your test suite

## Summary

DLQ testing verifies that:
- ✅ Failed messages are not lost
- ✅ Error information is preserved
- ✅ Messages can be reprocessed after fixing issues
- ✅ System handles failures gracefully

Use the tests above to ensure your DLQ implementation is working correctly!
