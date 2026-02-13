"""
Kafka Admin Client for managing topics, partitions, and consumer groups.

This module provides utilities for creating, deleting, and inspecting Kafka topics
used in the CDC (Change Data Capture) pipeline.
"""

from confluent_kafka.admin import AdminClient, NewTopic, ConfigResource


class cdcClient(AdminClient):
    """
    AdminClient wrapper for managing Kafka topics and consumer groups.
    
    Provides methods for topic creation, deletion, existence checking,
    and consumer group inspection.
    """
    
    def __init__(self):
        """
        Initialize the Kafka Admin Client.
        Connects to Kafka broker at localhost:29092 (default for local development).
        """
        config = {'bootstrap.servers': 'localhost:29092'}
        super().__init__(config)

    def topic_exists(self, topic):
        """
        Check if a Kafka topic exists.
        
        Args:
            topic (str): Name of the topic to check
            
        Returns:
            bool: True if topic exists, False otherwise
        """
        metadata = self.list_topics()
        for t in iter(metadata.topics.values()):
            if t.topic == topic:
                return True
        return False

    def create_topic(self, topic, num_partitions):
        """
        Create a new Kafka topic with the specified number of partitions.
        
        Args:
            topic (str): Name of the topic to create
            num_partitions (int): Number of partitions for the topic
            
        Note:
            Replication factor is set to 1 since we only have 1 broker in docker-compose
        """
        # Create topic with specified partitions and replication factor
        # Replication factor is 1 because we only have 1 broker in docker-compose.yml
        new_topic = NewTopic(topic, num_partitions=num_partitions, replication_factor=1)
        result_dict = self.create_topics([new_topic])
        
        # Wait for topic creation to complete and handle results
        for topic_name, future in result_dict.items():
            try:
                future.result()  # Wait for the operation to complete
                print("Topic {} created with {} partitions".format(topic_name, num_partitions))
            except Exception as e:
                print("Failed to create topic {}: {}".format(topic_name, e))

    def get_consumer_group_size(self, group_id):
        """
        Get the number of consumers in a consumer group.
        
        Args:
            group_id (str): The consumer group ID to inspect
            
        Returns:
            int: Number of consumers in the group, or 0 if group doesn't exist
        """
        # Fetch consumer group metadata
        group_metadata = self.list_groups(group=group_id)
        
        # Extract and return the number of members (consumers) in the group
        print(f"Consumer group metadata: {group_metadata}")
        for group in group_metadata:
            if group.id == group_id:
                return len(group.members)
        return 0

    def delete_topic(self, topics):
        """
        Delete one or more Kafka topics.
        
        Args:
            topics (list): List of topic names to delete
        """
        fs = self.delete_topics(topics, operation_timeout=5)
        
        # Wait for deletion operations to complete
        for topic, f in fs.items():
            try:
                f.result()  # Wait for the operation to complete
                print("Topic {} deleted".format(topic))
            except Exception as e:
                print("Failed to delete topic {}: {}".format(topic, e))


if __name__ == '__main__':
    """
    Example usage: Create or recreate the employee CDC topic.
    This script can be run to set up the Kafka topic before starting the producer/consumer.
    """
    client = cdcClient()
    employee_topic_name = "bf_employee_cdc"
    num_partitions = 3
    
    # Delete topic if it exists, then create it fresh
    if client.topic_exists(employee_topic_name):
        print(f"Topic {employee_topic_name} exists. Deleting...")
        client.delete_topic([employee_topic_name])
    
    # Create the topic
    print(f"Creating topic {employee_topic_name} with {num_partitions} partitions...")
    client.create_topic(employee_topic_name, num_partitions)
    