# BFS_DE_Kafka_team4
# Real-Time Database Replication with Kafka CDC

A Change Data Capture (CDC) pipeline that maintains real-time synchronization between two PostgreSQL databases using Apache Kafka and Python.

## Overview

This project implements a **streaming data replication system** that captures changes (INSERT, UPDATE, DELETE) from a source PostgreSQL database and replicates them to a destination database in under 1 second. The system uses:

- **PostgreSQL Triggers** for change detection
- **Apache Kafka** for reliable message streaming
- **Python Producers/Consumers** for data pipeline orchestration

---
# Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     CDC Pipeline Flow                           │
└─────────────────────────────────────────────────────────────────┘

    Source Database          Kafka Cluster       Destination Database
    (PostgreSQL)             (Topics)            (PostgreSQL)
         │                       │                       │
         │                       │                       │
    ┌────▼────┐             ┌────▼────┐            ┌────▼────┐
    │employees│             │         │            │employees│
    │  table  │             │  Topic  │            │  table  │
    └────┬────┘             │bf_emp..│            └────▲────┘
         │                  │         │                 │
    ┌────▼────┐             └────┬────┘                │
    │Trigger  │                  │                     │
    │Function │                  │                     │
    └────┬────┘                  │                     │
         │                       │                     │
    ┌────▼────┐             ┌────▼────┐          ┌────┴────┐
    │emp_cdc  │───Producer──▶ Messages ├──Consumer▶ Apply   │
    │  table  │             │  Stream  │          │ Changes │
    └─────────┘             └──────────┘          └─────────┘
         │                       │                     │
         │                       │                     │
    ┌────▼────┐             ┌────▼────┐          ┌────▼────┐
    │cdc_     │             │  DLQ    │          │ Synced  │
    │offset   │             │ Topic   │          │  Data   │
    └─────────┘             └─────────┘          └─────────┘
```

---

## Features

- ✅ **Real-time Replication** - Changes propagate in <1 second
- ✅ **Full CDC Support** - Captures INSERT, UPDATE, and DELETE operations
- ✅ **Offset Management** - Tracks last processed change to prevent data loss
- ✅ **Fault Tolerance** - Kafka ensures reliable message delivery
- ✅ **Dead Letter Queue** - Optional validation and error handling
- ✅ **Scalable Architecture** - Easily add more consumers for horizontal scaling
- ✅ **Docker Compose** - Complete environment in one command

---

# How It Works

### 1. **Change Detection (PostgreSQL Triggers)**

When data changes in the `employees` table:
```sql
INSERT INTO employees (first_name, last_name, dob, city, salary)
VALUES ('Alice', 'Johnson', '1992-03-10', 'Boston', 90000);
```

A trigger automatically captures this:
```sql
-- Trigger inserts into emp_cdc table
INSERT INTO emp_cdc (emp_id, first_name, last_name, dob, city, salary, action)
VALUES (1, 'Alice', 'Johnson', '1992-03-10', 'Boston', 90000, 'INSERT');
```

### 2. **Producer Scans CDC Table**

The producer continuously polls `emp_cdc`:
```python
# Fetch new records since last offset
SELECT * FROM emp_cdc WHERE action_id > last_offset ORDER BY action_id ASC;
```

For each new record:
- Serialize to JSON
- Send to Kafka topic `bf_employee_cdc`
- Update offset checkpoint

### 3. **Consumer Applies Changes**

The consumer reads from Kafka and applies changes:
```python
if action == 'INSERT':
    # Insert into destination DB
elif action == 'UPDATE':
    # Update destination DB
elif action == 'DELETE':
    # Delete from destination DB
```

---

## Testing

### Test Scenario 1: Bulk Insert
```sql
INSERT INTO employees (first_name, last_name, dob, city, salary)
SELECT 
    'Employee_' || i,
    'LastName_' || i,
    '1990-01-01'::date + (i || ' days')::interval,
    'City_' || (i % 10),
    50000 + (i * 1000)
FROM generate_series(1, 100) as i;
```

### Test Scenario 2: Concurrent Updates
```sql
UPDATE employees SET salary = salary * 1.1 WHERE city = 'New York';
```

### Test Scenario 3: Mass Delete
```sql
DELETE FROM employees WHERE salary < 60000;
```
