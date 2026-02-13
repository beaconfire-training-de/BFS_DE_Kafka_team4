"""
Quick DLQ Testing Script

This script provides utilities to test and verify DLQ functionality.
Run this while producer.py and consumer.py are running.
"""

import psycopg2
import subprocess
import json
import os
import sys
import time


def get_kafka_container():
    """Get Kafka container ID."""
    try:
        result = subprocess.run(
            ["docker", "compose", "ps", "-q", "kafka"],
            capture_output=True,
            text=True,
            cwd=os.getcwd()
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
        return None
    except Exception as e:
        print(f"Error getting Kafka container: {e}")
        return None


def check_dlq_topic():
    """Check if DLQ topic exists."""
    print("=" * 60)
    print("Checking DLQ Topic")
    print("=" * 60)
    
    kafka_container = get_kafka_container()
    if not kafka_container:
        print("Kafka container not found")
        return False
    
    try:
        result = subprocess.run(
            ["docker", "exec", kafka_container,
             "kafka-topics", "--list", "--bootstrap-server", "kafka:9092"],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.returncode == 0:
            topics = result.stdout.strip().split('\n')
            if 'bf_employee_cdc_dlq' in topics:
                print(" DLQ topic 'bf_employee_cdc_dlq' exists")
                return True
            else:
                print("DLQ topic not found")
                print(f"  Available topics: {topics}")
                return False
        else:
            print(f"Failed to list topics: {result.stderr}")
            return False
    except Exception as e:
        print(f"Error checking DLQ topic: {e}")
        return False


def view_dlq_messages(count=5):
    """View messages from DLQ topic."""
    print("\n" + "=" * 60)
    print(f"Viewing DLQ Messages (last {count})")
    print("=" * 60)
    
    kafka_container = get_kafka_container()
    if not kafka_container:
        print("Kafka container not found")
        return False
    
    try:
        result = subprocess.run(
            ["docker", "exec", kafka_container,
             "kafka-console-consumer",
             "--bootstrap-server", "kafka:9092",
             "--topic", "bf_employee_cdc_dlq",
             "--from-beginning",
             "--max-messages", str(count),
             "--timeout-ms", "5000"],
            capture_output=True,
            text=True,
            timeout=15
        )
        
        if result.returncode == 0 and result.stdout.strip():
            messages = result.stdout.strip().split('\n')
            print(f"Found {len(messages)} DLQ message(s):\n")
            
            for i, msg in enumerate(messages, 1):
                if msg.strip():
                    print(f"--- Message {i} ---")
                    try:
                        # Try to pretty print JSON
                        msg_json = json.loads(msg)
                        print(json.dumps(msg_json, indent=2))
                    except:
                        print(msg)
                    print()
            return True
        else:
            print(" No DLQ messages found")
            print("  This is OK if no failures have occurred")
            return True  # Not a failure
            
    except Exception as e:
        print(f"Error viewing DLQ messages: {e}")
        return False


def count_dlq_messages():
    """Count messages in DLQ topic."""
    print("\n" + "=" * 60)
    print("Counting DLQ Messages")
    print("=" * 60)
    
    kafka_container = get_kafka_container()
    if not kafka_container:
        print("Kafka container not found")
        return False
    
    try:
        result = subprocess.run(
            ["docker", "exec", kafka_container,
             "kafka-run-class", "kafka.tools.GetOffsetShell",
             "--broker-list", "kafka:9092",
             "--topic", "bf_employee_cdc_dlq",
             "--time", "-1"],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.returncode == 0:
            lines = result.stdout.strip().split('\n')
            total = 0
            for line in lines:
                if ':' in line:
                    parts = line.split(':')
                    if len(parts) >= 3:
                        total += int(parts[-1])
            print(f"Total DLQ messages: {total}")
            return True
        else:
            print(f"Failed to count messages: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"Error counting DLQ messages: {e}")
        return False


def check_db_dst_status():
    """Check if destination database container is running."""
    try:
        result = subprocess.run(
            ["docker", "compose", "ps", "-q", "db_dst"],
            capture_output=True,
            text=True,
            cwd=os.getcwd()
        )
        if result.returncode == 0 and result.stdout.strip():
            container_id = result.stdout.strip()
            # Check if container is actually running
            status_result = subprocess.run(
                ["docker", "ps", "--filter", f"id={container_id}", "--format", "{{.Status}}"],
                capture_output=True,
                text=True
            )
            return status_result.stdout.strip() != ""
        return False
    except Exception as e:
        print(f"Error checking database status: {e}")
        return False


def stop_db_dst():
    """Stop the destination database container."""
    try:
        print("Stopping destination database (db_dst)...")
        result = subprocess.run(
            ["docker", "compose", "stop", "db_dst"],
            capture_output=True,
            text=True,
            cwd=os.getcwd()
        )
        if result.returncode == 0:
            print("✓ Destination database stopped")
            time.sleep(2)  # Give it a moment to fully stop
            return True
        else:
            print(f"Failed to stop database: {result.stderr}")
            return False
    except Exception as e:
        print(f"Error stopping database: {e}")
        return False


def start_db_dst():
    """Start the destination database container."""
    try:
        print("Starting destination database (db_dst)...")
        result = subprocess.run(
            ["docker", "compose", "start", "db_dst"],
            capture_output=True,
            text=True,
            cwd=os.getcwd()
        )
        if result.returncode == 0:
            print("✓ Destination database started")
            time.sleep(3)  # Give it a moment to fully start
            return True
        else:
            print(f"Failed to start database: {result.stderr}")
            return False
    except Exception as e:
        print(f"Error starting database: {e}")
        return False


def test_consumer_dlq_scenario():
    """Automatically test consumer DLQ by simulating database failure."""
    print("\n" + "=" * 60)
    print("Consumer DLQ Test Scenario - Automated")
    print("=" * 60)
    print("\nThis test simulates a database failure scenario.")
    print("Steps:")
    print("1. Verify producer and consumer are running")
    print("2. Stop destination database")
    print("3. Insert a test record in source database")
    print("4. Wait for consumer retry attempts and DLQ message")
    print("5. Check DLQ for the failed message")
    print("6. Restart database")
    
    # Check if database is running
    db_was_running = check_db_dst_status()
    
    if db_was_running:
        print("\n✓ Destination database is running - will stop it for testing")
    else:
        print("\n⚠️  Destination database is already stopped")
    
    # Stop database if running
    if db_was_running:
        if not stop_db_dst():
            print("Failed to stop database. Cannot proceed with test.")
            return False
    
    # Insert test record
    test_emp_id = 7777
    print(f"\nInserting test record (emp_id={test_emp_id})...")
    
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="postgres",
            user="postgres",
            password="postgres"
        )
        cur = conn.cursor()
        
        # Clean up if exists
        cur.execute("DELETE FROM employees WHERE emp_id = %s", (test_emp_id,))
        cur.execute("DELETE FROM emp_cdc WHERE emp_id = %s", (test_emp_id,))
        conn.commit()
        
        # Insert test record
        cur.execute("""
            INSERT INTO employees (emp_id, first_name, last_name, dob, city, salary)
            VALUES (%s, 'DLQ', 'Test', '1990-01-01', 'Test City', 50000)
        """, (test_emp_id,))
        conn.commit()
        
        cur.close()
        conn.close()
        
        print(f"Test record inserted (emp_id={test_emp_id})")
        
        # Wait a bit for producer to detect and publish
        print("\nWaiting 5 seconds for producer to detect and publish the message...")
        time.sleep(5)
        
        # Verify message was published to main topic
        print("\nVerifying message was published to main topic...")
        kafka_container = get_kafka_container()
        message_published = False
        if kafka_container:
            try:
                result = subprocess.run(
                    ["docker", "exec", kafka_container,
                     "kafka-console-consumer",
                     "--bootstrap-server", "kafka:9092",
                     "--topic", "bf_employee_cdc",
                     "--from-beginning",
                     "--max-messages", "10",
                     "--timeout-ms", "3000"],
                    capture_output=True,
                    text=True,
                    timeout=10
                )
                if result.returncode == 0 and result.stdout.strip():
                    messages = result.stdout.strip().split('\n')
                    for msg in messages:
                        if str(test_emp_id) in msg:
                            print(f"Message found in main topic (emp_id={test_emp_id})")
                            message_published = True
                            break
                    if not message_published:
                        print(" Message not found in main topic yet")
                        print("  Make sure producer.py is running")
                else:
                    print("Could not verify message in main topic")
            except Exception as e:
                print(f"Error checking main topic: {e}")
        
        if not message_published:
            print("\nWarning: Message may not have been published yet.")
            print("  Make sure producer.py is running and polling for changes")
        
        print("\nWaiting for consumer to process (retries + DLQ)...")
        print("  Consumer will retry 3 times with exponential backoff")
        print("  Retry delays: 2s + 4s + 8s = 14s, plus processing time")
        print("  Total wait time: ~30 seconds")
        
        # Wait for retries: 2^1 + 2^2 + 2^3 = 2 + 4 + 8 = 14 seconds
        # Plus processing time, so wait 30 seconds to be safe
        # Check multiple times during wait
        for i in range(6):
            time.sleep(5)
            print(f"  Waiting... ({i+1}/6)")
            
            # Quick check if DLQ has messages now
            try:
                kafka_container = get_kafka_container()
                if kafka_container:
                    result = subprocess.run(
                        ["docker", "exec", kafka_container,
                         "kafka-run-class", "kafka.tools.GetOffsetShell",
                         "--broker-list", "kafka:9092",
                         "--topic", "bf_employee_cdc_dlq",
                         "--time", "-1"],
                        capture_output=True,
                        text=True,
                        timeout=5
                    )
                    if result.returncode == 0:
                        lines = result.stdout.strip().split('\n')
                        current_count = 0
                        for line in lines:
                            if ':' in line:
                                parts = line.split(':')
                                if len(parts) >= 3:
                                    current_count += int(parts[-1])
                        if current_count > dlq_count_before:
                            print(f"  DLQ message detected! (count: {current_count})")
                            break
            except:
                pass
        
        # Check DLQ
        print("\nChecking DLQ for the failed message...")
        dlq_count_before = 0
        try:
            # Get count before
            kafka_container = get_kafka_container()
            if kafka_container:
                result = subprocess.run(
                    ["docker", "exec", kafka_container,
                     "kafka-run-class", "kafka.tools.GetOffsetShell",
                     "--broker-list", "kafka:9092",
                     "--topic", "bf_employee_cdc_dlq",
                     "--time", "-1"],
                    capture_output=True,
                    text=True,
                    timeout=10
                )
                if result.returncode == 0:
                    lines = result.stdout.strip().split('\n')
                    for line in lines:
                        if ':' in line:
                            parts = line.split(':')
                            if len(parts) >= 3:
                                dlq_count_before += int(parts[-1])
        except:
            pass
        
        view_dlq_messages(count=5)
        
        # Check if new message appeared
        dlq_count_after = 0
        try:
            kafka_container = get_kafka_container()
            if kafka_container:
                result = subprocess.run(
                    ["docker", "exec", kafka_container,
                     "kafka-run-class", "kafka.tools.GetOffsetShell",
                     "--broker-list", "kafka:9092",
                     "--topic", "bf_employee_cdc_dlq",
                     "--time", "-1"],
                    capture_output=True,
                    text=True,
                    timeout=10
                )
                if result.returncode == 0:
                    lines = result.stdout.strip().split('\n')
                    for line in lines:
                        if ':' in line:
                            parts = line.split(':')
                            if len(parts) >= 3:
                                dlq_count_after += int(parts[-1])
        except:
            pass
        
        # Determine test result
        if dlq_count_after > dlq_count_before:
            print("\n" + "=" * 60)
            print("Consumer DLQ test PASSED!")
            print(f"  Found {dlq_count_after - dlq_count_before} new message(s) in DLQ")
            print("=" * 60)
            test_passed = True
        else:
            print("\n" + "=" * 60)
            print("Consumer DLQ test - No new messages in DLQ")
            print("=" * 60)
            print("\nPossible reasons:")
            print("  1. Consumer is not running")
            print("     → Start consumer: python consumer.py")
            print("  2. Producer hasn't published the message yet")
            print("     → Start producer: python producer.py")
            print("     → Check producer logs for 'Published CDC record'")
            print("  3. Message is still being retried")
            print("     → Wait longer and check consumer logs")
            print("  4. Message was processed successfully")
            print("     → Unlikely with DB stopped, but check consumer logs")
            print("\nTroubleshooting steps:")
            print("  - Check if message exists in main topic:")
            print(f"    docker exec {kafka_container if kafka_container else '<kafka-container>'} \\")
            print("      kafka-console-consumer --bootstrap-server kafka:9092 \\")
            print("      --topic bf_employee_cdc --from-beginning --max-messages 10")
            print("  - Check consumer logs for retry attempts")
            print("  - Verify database is stopped: docker-compose ps db_dst")
            print("=" * 60)
            test_passed = False
        
        # Restart database
        if db_was_running:
            print("\nRestarting destination database...")
            start_db_dst()
        
        return test_passed
            
    except Exception as e:
        print(f"Error in consumer DLQ test: {e}")
        # Try to restart database even on error
        if db_was_running:
            print("\nAttempting to restart database after error...")
            start_db_dst()
        return False


def main():
    """Run all DLQ tests automatically."""
    print("\n" + "=" * 60)
    print("DLQ Testing Utility - Running All Checks")
    print("=" * 60)
    
    # Run all checks automatically
    print("\nRunning all DLQ checks...\n")
    
    # 1. Check DLQ topic exists
    check_dlq_topic()
    
    # 2. Count DLQ messages
    count_dlq_messages()
    
    # 3. View DLQ messages (last 5)
    view_dlq_messages(5)
    
    # 4. Run automated DLQ test scenario
    print("\n" + "=" * 60)
    print("Running Automated DLQ Test Scenario")
    print("=" * 60)
    test_consumer_dlq_scenario()
    
    print("\n" + "=" * 60)
    print("All checks completed!")
    print("=" * 60)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nExiting...")
        sys.exit(0)
