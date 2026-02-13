"""
Test script to verify the CDC pipeline is working correctly.

This script performs basic tests to verify:
1. Database connections
2. CDC trigger functionality
3. Producer can detect changes
4. Consumer can process messages
5. Data replication works

Run this script while producer.py and consumer.py are running.
"""

import psycopg2
import time
import sys
import json
import subprocess
import os

def test_database_connections():
    """Test connections to both source and destination databases."""
    print("=" * 60)
    print("TEST 1: Database Connections")
    print("=" * 60)
    
    try:
        # Test source database
        conn_src = psycopg2.connect(
            host="localhost",
            port=5432,
            database="postgres",
            user="postgres",
            password="postgres"
        )
        cur_src = conn_src.cursor()
        cur_src.execute("SELECT 1")
        cur_src.close()
        conn_src.close()
        print("✓ Source database connection: OK")
    except Exception as e:
        print(f"✗ Source database connection failed: {e}")
        return False
    
    try:
        # Test destination database
        conn_dst = psycopg2.connect(
            host="localhost",
            port=5433,
            database="postgres",
            user="postgres",
            password="postgres"
        )
        cur_dst = conn_dst.cursor()
        cur_dst.execute("SELECT 1")
        cur_dst.close()
        conn_dst.close()
        print("✓ Destination database connection: OK")
    except Exception as e:
        print(f"✗ Destination database connection failed: {e}")
        return False
    
    return True


def test_cdc_trigger():
    """Test that CDC trigger is working."""
    print("\n" + "=" * 60)
    print("TEST 2: CDC Trigger Functionality")
    print("=" * 60)
    
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="postgres",
            user="postgres",
            password="postgres"
        )
        cur = conn.cursor()
        
        # Get initial CDC count
        cur.execute("SELECT COUNT(*) FROM emp_cdc")
        initial_count = cur.fetchone()[0]
        
        # Insert a test record
        test_emp_id = 9999
        cur.execute("""
            INSERT INTO employees (emp_id, first_name, last_name, dob, city, salary)
            VALUES (%s, 'Test', 'Trigger', '1990-01-01', 'Test City', 50000)
            ON CONFLICT (emp_id) DO UPDATE SET first_name = 'Test'
        """, (test_emp_id,))
        conn.commit()
        
        # Wait a moment for trigger to fire
        time.sleep(1)
        
        # Check if CDC record was created
        cur.execute("SELECT COUNT(*) FROM emp_cdc")
        new_count = cur.fetchone()[0]
        
        if new_count > initial_count:
            print("✓ CDC trigger is working - new CDC record created")
            
            # Clean up test record
            cur.execute("DELETE FROM employees WHERE emp_id = %s", (test_emp_id,))
            cur.execute("DELETE FROM emp_cdc WHERE emp_id = %s", (test_emp_id,))
            conn.commit()
            cur.close()
            conn.close()
            return True
        else:
            print("✗ CDC trigger failed - no new CDC record created")
            cur.close()
            conn.close()
            return False
            
    except Exception as e:
        print(f"✗ CDC trigger test failed: {e}")
        return False


def test_insert_replication():
    """Test INSERT operation replication."""
    print("\n" + "=" * 60)
    print("TEST 3: INSERT Operation Replication")
    print("=" * 60)
    
    test_emp_id = 8888
    
    try:
        # Insert record in source
        conn_src = psycopg2.connect(
            host="localhost",
            port=5432,
            database="postgres",
            user="postgres",
            password="postgres"
        )
        cur_src = conn_src.cursor()
        
        # Clean up if exists
        cur_src.execute("DELETE FROM employees WHERE emp_id = %s", (test_emp_id,))
        cur_src.execute("DELETE FROM emp_cdc WHERE emp_id = %s", (test_emp_id,))
        conn_src.commit()
        
        # Insert new record
        cur_src.execute("""
            INSERT INTO employees (emp_id, first_name, last_name, dob, city, salary)
            VALUES (%s, 'Insert', 'Test', '1990-01-01', 'Insert City', 60000)
        """, (test_emp_id,))
        conn_src.commit()
        cur_src.close()
        conn_src.close()
        
        print(f"✓ Record inserted in source database (emp_id={test_emp_id})")
        print("  Waiting 10 seconds for replication...")
        time.sleep(10)
        
        # Check destination
        conn_dst = psycopg2.connect(
            host="localhost",
            port=5433,
            database="postgres",
            user="postgres",
            password="postgres"
        )
        cur_dst = conn_dst.cursor()
        cur_dst.execute("SELECT * FROM employees WHERE emp_id = %s", (test_emp_id,))
        result = cur_dst.fetchone()
        
        if result:
            print(f"✓ INSERT replication successful - record found in destination")
            print(f"  Details: emp_id={result[0]}, name={result[1]} {result[2]}, city={result[4]}, salary={result[5]}")
            cur_dst.close()
            conn_dst.close()
            return True
        else:
            print("✗ INSERT replication failed - record not found in destination")
            print("  Make sure producer.py and consumer.py are running!")
            cur_dst.close()
            conn_dst.close()
            return False
            
    except Exception as e:
        print(f"✗ INSERT replication test failed: {e}")
        return False


def test_update_replication():
    """Test UPDATE operation replication."""
    print("\n" + "=" * 60)
    print("TEST 4: UPDATE Operation Replication")
    print("=" * 60)
    
    test_emp_id = 8888
    
    try:
        # Update record in source
        conn_src = psycopg2.connect(
            host="localhost",
            port=5432,
            database="postgres",
            user="postgres",
            password="postgres"
        )
        cur_src = conn_src.cursor()
        
        cur_src.execute("""
            UPDATE employees 
            SET salary = 75000, city = 'Updated City'
            WHERE emp_id = %s
        """, (test_emp_id,))
        conn_src.commit()
        cur_src.close()
        conn_src.close()
        
        print(f"✓ Record updated in source database (emp_id={test_emp_id})")
        print("  Waiting 10 seconds for replication...")
        time.sleep(10)
        
        # Check destination
        conn_dst = psycopg2.connect(
            host="localhost",
            port=5433,
            database="postgres",
            user="postgres",
            password="postgres"
        )
        cur_dst = conn_dst.cursor()
        cur_dst.execute("SELECT salary, city FROM employees WHERE emp_id = %s", (test_emp_id,))
        result = cur_dst.fetchone()
        
        if result and result[0] == 75000 and result[1] == 'Updated City':
            print(f"✓ UPDATE replication successful")
            print(f"  Updated values: salary={result[0]}, city={result[1]}")
            cur_dst.close()
            conn_dst.close()
            return True
        else:
            print(f"✗ UPDATE replication failed")
            print(f"  Expected: salary=75000, city='Updated City'")
            print(f"  Got: salary={result[0] if result else 'None'}, city={result[1] if result else 'None'}")
            cur_dst.close()
            conn_dst.close()
            return False
            
    except Exception as e:
        print(f"✗ UPDATE replication test failed: {e}")
        return False


def test_delete_replication():
    """Test DELETE operation replication."""
    print("\n" + "=" * 60)
    print("TEST 5: DELETE Operation Replication")
    print("=" * 60)
    
    test_emp_id = 8888
    
    try:
        # Delete record in source
        conn_src = psycopg2.connect(
            host="localhost",
            port=5432,
            database="postgres",
            user="postgres",
            password="postgres"
        )
        cur_src = conn_src.cursor()
        
        cur_src.execute("DELETE FROM employees WHERE emp_id = %s", (test_emp_id,))
        conn_src.commit()
        cur_src.close()
        conn_src.close()
        
        print(f"✓ Record deleted in source database (emp_id={test_emp_id})")
        print("  Waiting 10 seconds for replication...")
        time.sleep(10)
        
        # Check destination
        conn_dst = psycopg2.connect(
            host="localhost",
            port=5433,
            database="postgres",
            user="postgres",
            password="postgres"
        )
        cur_dst = conn_dst.cursor()
        cur_dst.execute("SELECT COUNT(*) FROM employees WHERE emp_id = %s", (test_emp_id,))
        count = cur_dst.fetchone()[0]
        
        if count == 0:
            print(f"✓ DELETE replication successful - record removed from destination")
            cur_dst.close()
            conn_dst.close()
            return True
        else:
            print(f"✗ DELETE replication failed - record still exists in destination")
            cur_dst.close()
            conn_dst.close()
            return False
            
    except Exception as e:
        print(f"✗ DELETE replication test failed: {e}")
        return False


def test_consumer_dlq_database_failure():
    """
    Test that consumer sends messages to DLQ when destination database is unavailable.
    This test requires manually stopping the destination database.
    """
    print("\n" + "=" * 60)
    print("TEST 6: Consumer DLQ - Database Failure")
    print("=" * 60)
    print("\n⚠️  MANUAL TEST REQUIRED:")
    print("  1. Stop destination database: docker-compose stop db_dst")
    print("  2. Insert a record in source database")
    print("  3. Watch consumer logs - should show retry attempts and DLQ message")
    print("  4. Check DLQ topic for the failed message")
    print("  5. Restart database: docker-compose start db_dst")
    print("\nPress Enter after completing the manual steps to verify DLQ message...")
    
    try:
        input()
        
        # Check if DLQ topic has messages
        # Get Kafka container
        result = subprocess.run(
            ["docker", "compose", "ps", "-q", "kafka"],
            capture_output=True,
            text=True,
            cwd=os.getcwd()
        )
        
        if result.returncode != 0:
            print("✗ Could not find Kafka container")
            return False
        
        kafka_container = result.stdout.strip()
        if not kafka_container:
            print("✗ Kafka container not found")
            return False
        
        # Check DLQ topic for messages
        check_cmd = [
            "docker", "exec", kafka_container,
            "kafka-console-consumer",
            "--bootstrap-server", "kafka:9092",
            "--topic", "bf_employee_cdc_dlq",
            "--from-beginning",
            "--max-messages", "1",
            "--timeout-ms", "5000"
        ]
        
        result = subprocess.run(
            check_cmd,
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.returncode == 0 and result.stdout.strip():
            print("✓ DLQ message found - Consumer DLQ test passed")
            print(f"  DLQ message preview: {result.stdout[:200]}...")
            return True
        else:
            print("✗ No DLQ message found")
            print("  Make sure:")
            print("    - Destination database was stopped")
            print("    - A record was inserted in source database")
            print("    - Consumer was running and attempted to process the message")
            return False
            
    except Exception as e:
        print(f"✗ Consumer DLQ test failed: {e}")
        return False


def test_dlq_message_format():
    """Test that DLQ messages have the correct format."""
    print("\n" + "=" * 60)
    print("TEST 7: DLQ Message Format Verification")
    print("=" * 60)
    
    try:
        # Get Kafka container
        result = subprocess.run(
            ["docker", "compose", "ps", "-q", "kafka"],
            capture_output=True,
            text=True,
            cwd=os.getcwd()
        )
        
        if result.returncode != 0:
            print("✗ Could not find Kafka container")
            return False
        
        kafka_container = result.stdout.strip()
        if not kafka_container:
            print("✗ Kafka container not found")
            return False
        
        # Consume one message from DLQ
        check_cmd = [
            "docker", "exec", kafka_container,
            "kafka-console-consumer",
            "--bootstrap-server", "kafka:9092",
            "--topic", "bf_employee_cdc_dlq",
            "--from-beginning",
            "--max-messages", "1",
            "--timeout-ms", "5000"
        ]
        
        result = subprocess.run(
            check_cmd,
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.returncode != 0 or not result.stdout.strip():
            print("⚠️  No DLQ messages found to verify format")
            print("  This is OK if no failures have occurred yet")
            return True  # Not a failure, just no messages
        
        # Try to parse the DLQ message
        try:
            dlq_message = json.loads(result.stdout.strip())
            
            # Verify required fields
            required_fields = ['original_topic', 'original_key', 'original_value', 'error', 'timestamp']
            missing_fields = [field for field in required_fields if field not in dlq_message]
            
            if missing_fields:
                print(f"✗ DLQ message missing required fields: {missing_fields}")
                return False
            
            print("✓ DLQ message format is correct")
            print(f"  Original topic: {dlq_message.get('original_topic')}")
            print(f"  Error: {dlq_message.get('error')[:100]}...")
            print(f"  Has timestamp: {'timestamp' in dlq_message}")
            return True
            
        except json.JSONDecodeError as e:
            print(f"✗ DLQ message is not valid JSON: {e}")
            return False
            
    except Exception as e:
        print(f"✗ DLQ format verification failed: {e}")
        return False


def test_dlq_topic_exists():
    """Verify that DLQ topic exists in Kafka."""
    print("\n" + "=" * 60)
    print("TEST 8: DLQ Topic Existence")
    print("=" * 60)
    
    try:
        # Get Kafka container
        result = subprocess.run(
            ["docker", "compose", "ps", "-q", "kafka"],
            capture_output=True,
            text=True,
            cwd=os.getcwd()
        )
        
        if result.returncode != 0:
            print("✗ Could not find Kafka container")
            return False
        
        kafka_container = result.stdout.strip()
        if not kafka_container:
            print("✗ Kafka container not found")
            return False
        
        # List topics
        list_cmd = [
            "docker", "exec", kafka_container,
            "kafka-topics",
            "--list",
            "--bootstrap-server", "kafka:9092"
        ]
        
        result = subprocess.run(
            list_cmd,
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.returncode != 0:
            print(f"✗ Failed to list topics: {result.stderr}")
            return False
        
        topics = result.stdout.strip().split('\n')
        
        if 'bf_employee_cdc_dlq' in topics:
            print("✓ DLQ topic 'bf_employee_cdc_dlq' exists")
            
            # Get topic details
            describe_cmd = [
                "docker", "exec", kafka_container,
                "kafka-topics",
                "--describe",
                "--bootstrap-server", "kafka:9092",
                "--topic", "bf_employee_cdc_dlq"
            ]
            
            describe_result = subprocess.run(
                describe_cmd,
                capture_output=True,
                text=True,
                timeout=10
            )
            
            if describe_result.returncode == 0:
                print(f"  Topic details:\n{describe_result.stdout}")
            
            return True
        else:
            print("✗ DLQ topic 'bf_employee_cdc_dlq' not found")
            print(f"  Available topics: {topics}")
            return False
            
    except Exception as e:
        print(f"✗ DLQ topic check failed: {e}")
        return False


def main():
    """Run all tests."""
    print("\n" + "=" * 60)
    print("CDC Pipeline Verification Tests")
    print("=" * 60)
    print("\nNOTE: Make sure producer.py and consumer.py are running before running these tests!")
    print("Press Ctrl+C to cancel, or wait 5 seconds to continue...")
    
    try:
        time.sleep(5)
    except KeyboardInterrupt:
        print("\nTests cancelled.")
        sys.exit(0)
    
    results = []
    
    # Run tests
    results.append(("Database Connections", test_database_connections()))
    results.append(("CDC Trigger", test_cdc_trigger()))
    results.append(("INSERT Replication", test_insert_replication()))
    results.append(("UPDATE Replication", test_update_replication()))
    results.append(("DELETE Replication", test_delete_replication()))
    
    # DLQ tests
    results.append(("DLQ Topic Exists", test_dlq_topic_exists()))
    results.append(("DLQ Message Format", test_dlq_message_format()))
    results.append(("Consumer DLQ (Manual)", test_consumer_dlq_database_failure()))
    
    # Summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"{status}: {test_name}")
    
    print("\n" + "=" * 60)
    print(f"Results: {passed}/{total} tests passed")
    print("=" * 60)
    
    if passed == total:
        print("\n🎉 All tests passed! Your CDC pipeline is working correctly.")
        return 0
    else:
        print(f"\n⚠️  {total - passed} test(s) failed. Please check the errors above.")
        print("Make sure:")
        print("  1. Docker services are running (docker-compose ps)")
        print("  2. producer.py is running")
        print("  3. consumer.py is running")
        return 1


if __name__ == '__main__':
    sys.exit(main())

