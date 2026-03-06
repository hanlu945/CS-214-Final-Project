from typing import Dict, Any, Optional

class Broker:
    """
    Kafka Broker managing N topics and M partitions.
    """
    def __init__(self):
        # TODO: Initializations for topic registries, storage, and retention config
        pass

    # ==========================================
    # EXTERNAL API
    # ==========================================

    def publishMsg(self, topic_id: str, payload_size: int, timestamp: int, producer_id: str, key: str) -> Dict[str, Any]:
        """
        Publish a new message to a topic. If the topic doesn't exist, it will be 
        created and assigned an initial priority tier.

        Args:
            topic_id (str): Name of the topic to publish to (e.g., "user-events")
            payload_size (int): Size of the message in bytes
            timestamp (int): Current simulation time step when message is created
            producer_id (str): ID of producer who sent the message
            key (str): Partitioning key for the message (Which entity this message belongs to)
        
        Returns:
            dict: 
                - success (bool): Whether publish succeeded
                - partition (int): Which partition the message was assigned to
                - offset (int): Message offset within the partition (sequential ID)
        """
        pass

    def consumeMsg(self, topic_id: str, partition: int, offset: int) -> Dict[str, Any]:
        """
        Consume a message for the consumer.
        
        Args:
            topic_id (str): Topic name
            partition (int): Partition number
            offset (int): Message offset
        
        Returns:
            dict:
                - success (bool): Whether consume succeeded
        """
        pass

    def editRetentionPolicy(self, mode: str, retention_sec: int, capacity_byte: int, eviction_batch_size: int) -> Dict[str, Any]:
        """
        Update the retention policy of the broker.
        
        Args: 
            mode (str): The retention policy mode to apply ('time' or 'lossy_priority')
            retention_sec (int): Max message lifetime in sec 
            capacity_byte (int): Storage capacity limit in bytes for lossy retention
            eviction_batch_size (int): Number of messages to evict per cleanup step

        Returns:
            dict:
                - success (bool): Whether the update succeeded
                - mode (str): Current retention mode
                - retention_sec (int): Current time retention setting
                - capacity_byte (int): Current capacity limit
                - eviction_batch_size (int): Current eviction batch size
                - error (str): Error message if failed    
        """
        pass

    def getStorageUsage(self) -> Dict[str, Any]:
        """
        Obtain current broker storage usage consumption for monitoring and evaluation.
        
        Returns:
            dict:
                - success (bool): Whether the query succeeded
                - total_storage (int): Total storage capacity of the broker (bytes)
                - used_storage (int): Current storage used (bytes) 
                - usage_ratio (float): used_storage / total_storage
        """
        pass

    # ==========================================
    # INTERNAL API
    # ==========================================

    def promoteTopic(self, topic_id: str) -> Dict[str, Any]:
        """
        Upgrade the priority tier for the topic.
        
        Returns:
            dict: success (bool), new_priority (int)
        """
        pass

    def demoteTopic(self, topic_id: str) -> Dict[str, Any]:
        """
        Downgrade the priority tier for the topic.
        
        Returns:
            dict: success (bool), new_priority (int)
        """
        pass

    def dropMsg(self, reason: str, now: int, bytes_needed: int = 0) -> Dict[str, Any]:
        """
        Delete message(s) when conditions of retention policy are met.
        
        Args: 
            reason (str): The reason for eviction ('time' or 'lossy_priority')
            now (int): Current timestamp used to check whether the message exceeds retention time
            bytes_needed (int): Number of bytes needed to be freed under storage pressure (default=0)

        Returns:
            dict:
                - dropped_count (int): Total number of messages removed
                - freed_bytes (int): Total amount of storage freed in bytes
                - dropped_topics (list/str): Topics from which messages were removed
                - reason (str): Time or lossy_priority    
        """
        pass

    def initialPriority(self, topic_id: str) -> Dict[str, Any]:
        """
        Assign initial tiers to topics.

        Returns: 
            dict:
                - success (bool): Whether the initialization was recorded 
                - priority (int): Initial priority of topic
        """
        pass

    def updateTopic(self, topic_id: str) -> Dict[str, Any]:
        """
        Update the priority tier of a topic based on stored revisit statistics. 
        Triggered periodically or before lossy eviction. Uses EMA scoring:
        score_t(topic) = λ * score_{t-1}(topic) + (1-λ) * recent_revisit_count(topic, t)

        Args:
           topic_id (str): Topic to evaluate

        Returns:
            dict:
                - success (bool): Whether the topic update succeeded
                - action_performed (str): "promote", "demote", or "unchanged"
                - new_priority (int): Updated priority tier of the topic
        """
        pass

    def recordRevisit(self, topic_id: str, partition: int, offset: int, timestamp: int) -> Dict[str, Any]:
        """
        Record one revisit event for a message.
        
        Args: 
            topic_id (str): The topic to which the revisited message belongs
            partition (int): The partition the message is stored in
            offset (int): Offset of revisited message
            timestamp (int): Time when the revisit happens

        Returns:
            dict:
                - success (bool): Whether the revisit was recorded 
                - msg_revisit_count (int): Updated revisit count of the message
                - error (str): Error message if failed    
        """
        pass

    def getMsg(self, topic_id: str, partition: int, offset: int) -> Dict[str, Any]:
        """
        Fetch a message by its exact coordinates.
        
        Args:
            topic_id (str): Topic name
            partition (int): Partition number
            offset (int): Message offset
            
        Returns:
            dict:  
                - success (bool): Whether fetch succeeded
                - topic_id (str): Name of the topic
                - payload_size (int): Size of the message in bytes
                - timestamp (int): Simulation time step when message was created
        """
        pass

    def getTopic(self, topic_id: str) -> Dict[str, Any]:
        """
        Fetch topic metadata from the topic registry.
        
        Args:
            topic_id (str): Topic name
            
        Returns: 
            dict: 
                - topic_id (str)
                - num_partitions (int)
                - priority (int)
                - total_messages (int)
        """
        pass

    def getPriority(self, topic_id: str) -> Dict[str, Any]:
        """
        Fetch priority level from the mesh/topic.
        
        Args:
            topic_id (str): Topic name
            
        Returns: 
            dict: 
                - success (bool)
                - priority (int)
        """
        pass