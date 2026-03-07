from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any


# =========================
# Core Data Classes
# =========================

@dataclass
class Message:
    offset: int
    payload_size: int
    timestamp: int
    topic_id: str
    partition_id: int
    producer_id: Optional[str] = None
    key: Optional[str] = None
    revisit_count: int = 0


@dataclass
class Partition:
    partition_id: int
    messages: List[Message] = field(default_factory=list)
    next_offset: int = 0

    def append_message(self, msg: Message) -> None:
        self.messages.append(msg)
        self.next_offset += 1


@dataclass
class Topic:
    topic_id: str
    priority: int
    num_partitions: int
    score: float = 0.0
    total_messages: int = 0
    revisit_count: int = 0
    revisit_history: List[int] = field(default_factory=list) # Time of revisit
    partitions: Dict[int, Partition] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for pid in range(self.num_partitions):
            self.partitions[pid] = Partition(partition_id=pid)


# =========================
# Broker
# =========================

class Broker:
    """
    Kafka data broker managing N topics and M partitions with 
    custom retention and priority-based eviction policies.
    """
    
    def __init__(
        self,
        total_storage: int,
        num_partitions_per_topic: int = 3,
        retention_mode: str = "time",
        retention_steps: Optional[int] = 100,
        capacity_byte: Optional[int] = None,
        eviction_batch_size: int = 1,
        lambda_weight: float = 0.8,
        t_mid: float = 5.0, # mid priority threshold
        t_high: float = 10.0, # high priority threshold
        window_length: int = 100,
    ) -> None:
        self.total_storage = total_storage
        self.used_storage = 0

        self.num_partitions_per_topic = num_partitions_per_topic

        self.retention_mode = retention_mode
        self.retention_steps = retention_steps
        self.capacity_byte = capacity_byte if capacity_byte is not None else total_storage
        self.eviction_batch_size = eviction_batch_size

        self.lambda_weight = lambda_weight
        self.t_mid = t_mid
        self.t_high = t_high
        self.window_length = window_length

        self.topic_registry: Dict[str, Topic] = {}

    # =========================
    # External API
    # =========================

    def publishMsg(
        self,
        topic_id: str,
        payload_size: int,
        timestamp: int,
        producer_id: Optional[str] = None,
        key: Optional[str] = None,
    ) -> dict:
        """
        Publish a new message to a topic. If the topic doesn't exist, it will be 
        created and assigned an initial priority tier.

        Args:
            topic_id (str): Name of the topic to publish to (e.g., "user-events")
            payload_size (int): Size of the message in bytes
            timestamp (int): Current simulation time step when message is created
            producer_id (Optional[str]): ID of producer who sent the message
            key (Optional[str]): Partitioning key for the message (Which entity this message belongs to)
        
        Returns:
            dict: 
                - success (bool): Whether publish succeeded
                - partition (int): Which partition the message was assigned to
                - offset (int): Message offset within the partition (sequential ID)
        """
        if topic_id not in self.topic_registry:
            init_result = self.initialPriority(topic_id)
            topic = Topic(
                topic_id=topic_id,
                priority=init_result["priority"],
                num_partitions=self.num_partitions_per_topic,
            )
            self.topic_registry[topic_id] = topic

        topic = self.topic_registry[topic_id]
        partition_id = self._choose_partition(topic, key)
        partition = topic.partitions[partition_id]

        msg = Message(
            offset=partition.next_offset,
            payload_size=payload_size,
            timestamp=timestamp,
            topic_id=topic_id,
            partition_id=partition_id,
            producer_id=producer_id,
            key=key,
        )

        partition.append_message(msg)
        topic.total_messages += 1
        self.used_storage += payload_size

        # Optional: trigger retention cleanup after publish, otherwise cleanup periodically
        if self.used_storage > self.capacity_byte:
            self.dropMsg(
                reason=self.retention_mode,
                now=timestamp,
                bytes_needed=self.used_storage - self.capacity_byte,
            )

        return {
            "success": True,
            "partition": partition_id,
            "offset": msg.offset,
        }

    def consumeMsg(self, topic_id: str, partition: int, offset: int, current_time: int) -> dict:
        """
        Consume a message for the consumer.
        Re-consuming a message counts as a revisit.
        
        Args:
            topic_id (str): Topic name
            partition (int): Partition number
            offset (int): Message offset
            current_time (int): Time step when the message is being consumed
        
        Returns:
            dict:
                - success (bool): Whether consume succeeded
                - topic_id (str): Topic name
                - payload_size (int): Size of the message payload
                - timestamp (int): Message creation timestamp
                - msg_revisit_count (int): Updated revisit count
        """
        msg_result = self.getMsg(topic_id, partition, offset)
        if not msg_result["success"]:
            return {"success": False}

        revisit_result = self.recordRevisit(
            topic_id=topic_id,
            partition=partition,
            offset=offset,
            timestamp=current_time
        )

        return {
            "success": True,
            "topic_id": msg_result["topic_id"],
            "payload_size": msg_result["payload_size"],
            "timestamp": msg_result["timestamp"],
            "msg_revisit_count": revisit_result["msg_revisit_count"],
        }

    def editRetentionPolicy(
        self,
        mode: str,
        retention_steps: Optional[int] = None,
        capacity_byte: Optional[int] = None,
        eviction_batch_size: Optional[int] = None,
    ) -> dict:
        """
        Update the retention policy of the broker.
        
        Args: 
            mode (str): The retention policy mode to apply ('time' or 'lossy_priority')
            retention_steps (Optional[int]): Max message lifetime in steps
            capacity_byte (Optional[int]): Storage capacity limit in bytes for lossy retention
            eviction_batch_size (Optional[int]): Number of messages to evict per cleanup step

        Returns:
            dict:
                - success (bool): Whether the update succeeded
                - mode (str): Current retention mode
                - retention_steps (int): Current time retention setting
                - capacity_byte (int): Current capacity limit
                - eviction_batch_size (int): Current eviction batch size
                - error (str): Error message if failed    
        """
        if mode not in {"time", "lossy_priority"}:
            return {
                "success": False,
                "error": "INVALID_RETENTION_MODE",
            }

        self.retention_mode = mode

        if retention_steps is not None:
            self.retention_steps = retention_steps
        if capacity_byte is not None:
            self.capacity_byte = capacity_byte
        if eviction_batch_size is not None:
            self.eviction_batch_size = eviction_batch_size

        return {
            "success": True,
            "mode": self.retention_mode,
            "retention_steps": self.retention_steps,
            "capacity_byte": self.capacity_byte,
            "eviction_batch_size": self.eviction_batch_size,
        }

    def getStorageUsage(self) -> dict:
        """
        Obtain current broker storage usage consumption for monitoring and evaluation.
        
        Returns:
            dict:
                - success (bool): Whether the query succeeded
                - total_storage (int): Total storage capacity of the broker (bytes)
                - used_storage (int): Current storage used (bytes) 
                - usage_ratio (float): used_storage / total_storage
        """
        return {
            "success": True,
            "total_storage": self.total_storage,
            "used_storage": self.used_storage,
            "usage_ratio": self.used_storage / self.total_storage if self.total_storage else 0.0,
        }

    # =========================
    # Internal API
    # =========================

    def promoteTopic(self, topic_id: str) -> dict:
        """
        Upgrade the priority tier for the topic.
        
        Returns:
            dict: 
                - success (bool): Whether promotion succeeded
                - new_priority (int): The updated priority tier
        """
        topic = self.topic_registry.get(topic_id)
        if topic is None:
            return {"success": False, "new_priority": None}

        topic.priority = min(topic.priority + 1, 2)
        return {"success": True, "new_priority": topic.priority}

    def demoteTopic(self, topic_id: str) -> dict:
        """
        Downgrade the priority tier for the topic.
        
        Returns:
            dict: 
                - success (bool): Whether demotion succeeded
                - new_priority (int): The updated priority tier
        """
        topic = self.topic_registry.get(topic_id)
        if topic is None:
            return {"success": False, "new_priority": None}

        topic.priority = max(topic.priority - 1, 0)
        return {"success": True, "new_priority": topic.priority}

    def dropMsg(
        self,
        reason: str,
        now: Optional[int] = None,
        bytes_needed: int = 0,
    ) -> dict:
        """
        Delete message(s) when conditions of retention policy are met.
        
        Args: 
            reason (str): The reason for eviction ('time' or 'lossy_priority')
            now (Optional[int]): Current timestamp used to check whether the message exceeds retention time
            bytes_needed (int): Number of bytes needed to be freed under storage pressure (default=0)

        Returns:
            dict:
                - dropped_count (int): Total number of messages removed
                - freed_bytes (int): Total amount of storage freed in bytes
                - dropped_topics (list/str): Topics from which messages were removed
                - reason (str): Time or lossy_priority    
        """
        dropped_count = 0
        freed_bytes = 0
        dropped_topics = set()

        if reason == "time":
            if now is None or self.retention_steps is None:
                return {
                    "dropped_count": 0,
                    "freed_bytes": 0,
                    "dropped_topics": [],
                    "reason": reason,
                }

            for topic in self.topic_registry.values():
                for partition in topic.partitions.values():
                    new_messages = []
                    for msg in partition.messages:
                        if now - msg.timestamp > self.retention_steps:
                            dropped_count += 1
                            freed_bytes += msg.payload_size
                            dropped_topics.add(topic.topic_id)
                            self.used_storage -= msg.payload_size
                        else:
                            new_messages.append(msg)
                    partition.messages = new_messages

        elif reason == "lossy_priority":
            # Before lossy eviction, refresh topic priorities
            for topic_id in self.topic_registry:
                self.updateTopic(topic_id)

            # Lower-priority topics should be dropped first
            sorted_topics = sorted(
                self.topic_registry.values(),
                key=lambda t: t.priority
            )

            for topic in sorted_topics:
                if freed_bytes >= bytes_needed:
                    break

                for partition in topic.partitions.values():
                    while partition.messages and freed_bytes < bytes_needed:
                        msg = partition.messages.pop(0)  # drop oldest first
                        dropped_count += 1
                        freed_bytes += msg.payload_size
                        dropped_topics.add(topic.topic_id)
                        self.used_storage -= msg.payload_size

                        if dropped_count % self.eviction_batch_size == 0 and freed_bytes >= bytes_needed:
                            break

                    if freed_bytes >= bytes_needed:
                        break

        return {
            "dropped_count": dropped_count,
            "freed_bytes": freed_bytes,
            "dropped_topics": list(dropped_topics),
            "reason": reason,
        }

    def initialPriority(self, topic_id: str) -> dict:
        """
        Assign initial tiers to topics. All topics start at the same initial 
        priority, and are later differentiated by revisit-based updates.

        Args:
            topic_id (str): The topic being initialized

        Returns: 
            dict:
                - success (bool): Whether the initialization was recorded 
                - priority (int): Initial priority of topic
        """
        return {
            "success": True,
            "priority": 0,
        }

    def updateTopic(self, topic_id: str) -> dict:
        """
        Update the priority tier of a topic based on stored revisit statistics. 
        Triggered periodically or before lossy eviction. Uses EMA scoring:
        
        $score_t(topic) = \lambda \cdot score_{t-1}(topic) + (1-\lambda) \cdot recent\_revisit\_count(topic, t)$

        Args:
           topic_id (str): Topic to evaluate

        Returns:
            dict:
                - success (bool): Whether the topic update succeeded
                - action_performed (str): "promote", "demote", or "unchanged"
                - new_priority (int): Updated priority tier of the topic
        """
        topic = self.topic_registry.get(topic_id)
        if topic is None:
            return {
                "success": False,
                "action_performed": "unchanged",
                "new_priority": None,
            }

        recent_count = self._recent_revisit_count(topic)
        old_priority = topic.priority

        topic.score = (
            self.lambda_weight * topic.score
            + (1 - self.lambda_weight) * recent_count
        )

        if topic.score >= self.t_high:
            new_priority = 2
        elif topic.score >= self.t_mid:
            new_priority = 1
        else:
            new_priority = 0

        topic.priority = new_priority

        if new_priority > old_priority:
            action = "promote"
        elif new_priority < old_priority:
            action = "demote"
        else:
            action = "unchanged"

        return {
            "success": True,
            "action_performed": action,
            "new_priority": topic.priority,
        }

    def recordRevisit(
        self,
        topic_id: str,
        partition: int,
        offset: int,
        timestamp: int,
    ) -> dict:
        """
        Record one revisit event for a message.
        This does NOT automatically update priority every time.
        Priority should be updated periodically or before lossy eviction.
        
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
        topic = self.topic_registry.get(topic_id)
        if topic is None:
            return {
                "success": False,
                "msg_revisit_count": None,
                "error": "TOPIC_NOT_FOUND",
            }

        partition_obj = topic.partitions.get(partition)
        if partition_obj is None:
            return {
                "success": False,
                "msg_revisit_count": None,
                "error": "PARTITION_NOT_FOUND",
            }

        target_msg = None
        for msg in partition_obj.messages:
            if msg.offset == offset:
                target_msg = msg
                break

        if target_msg is None:
            return {
                "success": False,
                "msg_revisit_count": None,
                "error": "MESSAGE_NOT_FOUND",
            }

        target_msg.revisit_count += 1
        topic.revisit_count += 1
        topic.revisit_history.append(timestamp)

        return {
            "success": True,
            "msg_revisit_count": target_msg.revisit_count,
            "error": None,
        }

    def getMsg(self, topic_id: str, partition: int, offset: int) -> dict:
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
        topic = self.topic_registry.get(topic_id)
        if topic is None:
            return {"success": False}

        partition_obj = topic.partitions.get(partition)
        if partition_obj is None:
            return {"success": False}

        for msg in partition_obj.messages:
            if msg.offset == offset:
                return {
                    "success": True,
                    "topic_id": msg.topic_id,
                    "payload_size": msg.payload_size,
                    "timestamp": msg.timestamp,
                }

        return {"success": False}

    def getTopic(self, topic_id: str) -> dict:
        """
        Fetch topic metadata from the topic registry.
        
        Args:
            topic_id (str): Topic name
            
        Returns: 
            dict: 
                - success (bool): Whether topic was found
                - topic_id (str): Name of the topic
                - num_partitions (int): Number of partitions in the topic
                - priority (int): Priority tier of the topic
                - total_messages (int): Total messages routed to the topic
        """
        topic = self.topic_registry.get(topic_id)
        if topic is None:
            return {"success": False}

        return {
            "success": True,
            "topic_id": topic.topic_id,
            "num_partitions": topic.num_partitions,
            "priority": topic.priority,
            "total_messages": topic.total_messages,
        }

    def getPriority(self, topic_id: str) -> dict:
        """
        Fetch priority level from the topic.
        
        Args:
            topic_id (str): Topic name
            
        Returns: 
            dict: 
                - success (bool): Whether topic was found
                - priority (int): Priority tier
        """
        topic = self.topic_registry.get(topic_id)
        if topic is None:
            return {
                "success": False,
                "priority": None,
            }

        return {
            "success": True,
            "priority": topic.priority,
        }

    # =========================
    # Helper Methods
    # =========================

    def _choose_partition(self, topic: Topic, key: Optional[str]) -> int:
        """
        Choose partition for a message.
        - If key is provided, hash by key
        - Otherwise, simple round-robin by total_messages
        """
        if key is not None:
            return hash(key) % topic.num_partitions
        return topic.total_messages % topic.num_partitions

    def _recent_revisit_count(self, topic: Topic) -> int:
        """
        Count revisit events within the recent window.
        Assumes revisit_history stores timestamps in ascending order.
        """
        if not topic.revisit_history:
            return 0

        current_time = topic.revisit_history[-1]
        lower_bound = current_time - self.window_length

        count = 0
        for ts in topic.revisit_history:
            if ts >= lower_bound:
                count += 1
        return count