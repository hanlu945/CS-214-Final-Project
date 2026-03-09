# =========================
# Simulation Experiment
# =========================

import random
from broker import Broker


class SimpleSimulation:
    """Simple simulation with configurable production and consumption rates"""
    
    def __init__(
        self,
        broker,
        duration=10000,
        publish_rate=10,      # Publish every N time steps
        revisit_prob=0.1,     # Probability of revisiting old message
        num_topics=3,         # Number of topics to create
        message_size_range=(500, 2000),
        topic_access_weights=None,  # NEW: Dict of topic_id -> weight
    ):
        self.broker = broker
        self.duration = duration
        self.publish_rate = publish_rate
        self.revisit_prob = revisit_prob
        self.num_topics = num_topics
        self.message_size_range = message_size_range
        self.topic_access_weights = topic_access_weights
        
        # Track published messages for revisits
        self.published_messages = []
        
        # Track published messages by topic (for weighted access)
        self.published_messages_by_topic = {}
        for i in range(num_topics):
            self.published_messages_by_topic[f"topic-{i}"] = []
        
        # Metrics
        self.total_published = 0
        self.cache_hits = 0
        self.cache_misses = 0
        self.storage_samples = []
    
    def run(self):
        """Run the simulation"""
        access_pattern = "WEIGHTED" if self.topic_access_weights else "UNIFORM"
        print(f"\n{'='*70}")
        print(f"Starting Simulation:")
        print(f"  Duration: {self.duration} time steps")
        print(f"  Publish rate: every {self.publish_rate} steps")
        print(f"  Revisit probability: {self.revisit_prob}")
        print(f"  Topics: {self.num_topics}")
        if self.topic_access_weights:
            print(f"  Access weights: {self.topic_access_weights}")
        print(f"{'='*70}\n")
        
        for time_step in range(self.duration):
            # Publish messages
            if time_step % self.publish_rate == 0:
                self.publish_messages(time_step)
            
            # Consume/revisit messages
            self.consume_messages(time_step)
            
            # Sample storage periodically
            if time_step % 100 == 0:
                self.sample_storage(time_step)
        
        # Print results
        self.print_results()
        return self.get_metrics()
    
    def publish_messages(self, time_step):
        """Publish messages to random topics"""
        # Choose random topic
        topic_id = f"topic-{random.randint(0, self.num_topics - 1)}"
        
        # Get message size from configured range
        min_size, max_size = self.message_size_range
        
        # Publish message
        result = self.broker.publishMsg(
            topic_id=topic_id,
            payload_size=random.randint(min_size, max_size),
            timestamp=time_step,
            producer_id="producer-1",
            key=None
        )
        
        if result['success']:
            self.total_published += 1
            msg_info = {
                'topic_id': topic_id,
                'partition': result['partition'],
                'offset': result['offset'],
                'time': time_step
            }
            # Save for potential revisit
            self.published_messages.append(msg_info)
            # Also save by topic
            self.published_messages_by_topic[topic_id].append(msg_info)
    
    def consume_messages(self, time_step):
        """Revisit old messages with optional topic-based weighting"""
        if not self.published_messages:
            return
        
        # Revisit with some probability
        if random.random() < self.revisit_prob:
            # Choose message based on topic access weights
            if self.topic_access_weights:
                old_msg = self._select_message_weighted()
            else:
                old_msg = random.choice(self.published_messages)
            
            if old_msg is None:
                return
            
            # Try to consume it
            result = self.broker.consumeMsg(
                topic_id=old_msg['topic_id'],
                partition=old_msg['partition'],
                offset=old_msg['offset'],
                current_time=time_step
            )
            
            if result['success']:
                self.cache_hits += 1
            else:
                self.cache_misses += 1

    def _select_message_weighted(self):
        """Select a message from topics based on access weights"""
        # Filter out topics with no published messages
        available_topics = [
            topic for topic in self.topic_access_weights.keys()
            if len(self.published_messages_by_topic.get(topic, [])) > 0
        ]
        
        if not available_topics:
            return None
        
        # Get weights for available topics
        weights = [self.topic_access_weights[topic] for topic in available_topics]
        
        # Normalize weights to probabilities
        total_weight = sum(weights)
        probabilities = [w / total_weight for w in weights]
        
        # Select topic based on weights
        selected_topic = random.choices(available_topics, weights=probabilities, k=1)[0]
        
        # Select random message from that topic
        topic_messages = self.published_messages_by_topic[selected_topic]
        return random.choice(topic_messages)
    
    def sample_storage(self, time_step):
        """Sample storage usage"""
        storage = self.broker.getStorageUsage()
        if storage['success']:
            self.storage_samples.append({
                'time': time_step,
                'used': storage['used_storage'],
                'ratio': storage['usage_ratio']
            })
            
            # Print progress
            if time_step % 500 == 0:
                total_revisits = self.cache_hits + self.cache_misses
                hit_rate = (self.cache_hits / total_revisits * 100) if total_revisits > 0 else 0
                print(f"[{time_step:5d}] Storage: {storage['used_storage']/1e6:6.2f} MB "
                      f"({storage['usage_ratio']*100:5.1f}%) | Hit Rate: {hit_rate:5.1f}%")
    
    def print_results(self):
        """Print final results"""
        print(f"\n{'='*70}")
        print("FINAL RESULTS")
        print(f"{'='*70}")
        print(f"Total messages published: {self.total_published}")
        
        total_revisits = self.cache_hits + self.cache_misses
        if total_revisits > 0:
            hit_rate = self.cache_hits / total_revisits * 100
            print(f"Cache hits: {self.cache_hits}")
            print(f"Cache misses: {self.cache_misses}")
            print(f"Cache hit rate: {hit_rate:.2f}%")
        
        if self.storage_samples:
            final = self.storage_samples[-1]
            print(f"Final storage used: {final['used'] / 1e6:.2f} MB")
            print(f"Final usage ratio: {final['ratio'] * 100:.1f}%")
        
        print(f"{'='*70}\n")
    
    def get_metrics(self):
        """Return metrics dict"""
        total_revisits = self.cache_hits + self.cache_misses
        hit_rate = (self.cache_hits / total_revisits) if total_revisits > 0 else 0
        
        return {
            'total_published': self.total_published,
            'cache_hits': self.cache_hits,
            'cache_misses': self.cache_misses,
            'hit_rate': hit_rate,
            'storage_samples': self.storage_samples
        }


def run_experiment(name, broker_config, sim_config):
    """Run a single experiment"""
    print(f"\n\n{'#'*70}")
    print(f"# EXPERIMENT: {name}")
    print(f"{'#'*70}")
    
    # Create broker
    broker = Broker(**broker_config)
    
    # Run simulation
    sim = SimpleSimulation(broker, **sim_config)
    metrics = sim.run()
    
    return metrics


def main():
    """
    Realistic Kafka Broker Simulation
    """
    
    # ============================================
    # CONFIGURATION
    # ============================================
    
    print("\n" + "="*70)
    print("RUNNING EXPERIMENTS")
    print("="*70)

    MESSAGE_SIZE_MIN = 500
    MESSAGE_SIZE_MAX = 2000

    # skewed_weights = {
    #     'topic-0': 3.5,  # High priority (35% of accesses)
    #     'topic-1': 3.5,  # High priority (35% of accesses)
    #     'topic-2': 1.0,  # Low priority (10%)
    #     'topic-3': 1.0,  # Low priority (10%)
    #     'topic-4': 1.0,  # Low priority (10%)
    # }

    skewed_weights = {
        'topic-0': 2.0,  # Equal priority (20%)
        'topic-1': 2.0,  # Equal priority (20%)
        'topic-2': 2.0,  # Equal priority (20%)
        'topic-3': 2.0,  # Equal priority (20%)
        'topic-4': 2.0,  # Equal priority (20%)
    }

    # Experiment 1: Time-based Retention (10 MB storage, 850 retention steps)
    experiment_1 = run_experiment(
        name="Time-based Retention (skewed, 10 MB storage, 850 retention steps)",
        broker_config={
            'total_storage': 10_000_000,   # 10 MB
            'num_partitions_per_topic': 3,
            'retention_mode': 'time',
            'retention_steps': 850,   
        },
        sim_config={
            'duration': 10_000,    
            'publish_rate': 1,      
            'revisit_prob': 0.33,    
            'num_topics': 5,
            'message_size_range': (MESSAGE_SIZE_MIN, MESSAGE_SIZE_MAX),
            'topic_access_weights': skewed_weights,  # SKEWED
        }
    )
    
    # Experiment 2: Lossy-priority Retention (10 MB storage, 20% Utilization)
    experiment_2 = run_experiment(
        name="Lossy-priority Retention (skewed, 10 MB storage, 20% Utilization)",
        broker_config={
            'total_storage': 10_000_000,   # 10 MB total
            'num_partitions_per_topic': 3,
            'retention_mode': 'lossy_priority',
            'capacity_byte': 2_000_000,    # 2 MB capacity
            'eviction_batch_size': 20,
        },
        sim_config={
            'duration': 10_000,
            'publish_rate': 1,            
            'revisit_prob': 0.33,         
            'num_topics': 5,
            'message_size_range': (MESSAGE_SIZE_MIN, MESSAGE_SIZE_MAX),
            'topic_access_weights': skewed_weights,  # SKEWED              
        }
    )

    # Experiment 3: Time-based Retention (10 MB storage, 1650 retention steps)
    experiment_3 = run_experiment(
        name="Time-based Retention (skewed, 10 MB storage, 1650 retention steps)",
        broker_config={
            'total_storage': 10_000_000,   # 10 MB
            'num_partitions_per_topic': 3,
            'retention_mode': 'time',
            'retention_steps': 1650,   
        },
        sim_config={
            'duration': 10_000,    
            'publish_rate': 1,      
            'revisit_prob': 0.33,    
            'num_topics': 5,
            'message_size_range': (MESSAGE_SIZE_MIN, MESSAGE_SIZE_MAX),
            'topic_access_weights': skewed_weights,  # SKEWED
        }
    )
    
    # Experiment 4: Lossy-priority Retention (10 MB storage, 40% Utilization)
    experiment_4 = run_experiment(
        name="Lossy-priority Retention (skewed, 10 MB storage, 40% Utilization)",
        broker_config={
            'total_storage': 10_000_000,   # 10 MB total
            'num_partitions_per_topic': 3,
            'retention_mode': 'lossy_priority',
            'capacity_byte': 4_000_000,    # 4 MB capacity
            'eviction_batch_size': 20,
        },
        sim_config={
            'duration': 10_000,
            'publish_rate': 1,            
            'revisit_prob': 0.33,         
            'num_topics': 5,
            'message_size_range': (MESSAGE_SIZE_MIN, MESSAGE_SIZE_MAX),
            'topic_access_weights': skewed_weights,  # SKEWED              
        }
    )
    
    # Experiment 5: Time-based Retention (10 MB storage, 2500 retention steps)
    experiment_5 = run_experiment(
        name="Time-based Retention (skewed, 10 MB storage, 2500 retention steps)",
        broker_config={
            'total_storage': 10_000_000,   # 10 MB
            'num_partitions_per_topic': 3,
            'retention_mode': 'time',
            'retention_steps': 2500,   
        },
        sim_config={
            'duration': 10_000,    
            'publish_rate': 1,      
            'revisit_prob': 0.33,    
            'num_topics': 5,
            'message_size_range': (MESSAGE_SIZE_MIN, MESSAGE_SIZE_MAX),
            'topic_access_weights': skewed_weights,  # SKEWED
        }
    )
    
    # Experiment 6: Lossy-priority Retention (10 MB storage, 60% Utilization)
    experiment_6 = run_experiment(
        name="Lossy-priority Retention (skewed, 10 MB storage, 60% Utilization)",
        broker_config={
            'total_storage': 10_000_000,   # 10 MB total
            'num_partitions_per_topic': 3,
            'retention_mode': 'lossy_priority',
            'capacity_byte': 6_000_000,    # 6 MB capacity
            'eviction_batch_size': 20,
        },
        sim_config={
            'duration': 10_000,
            'publish_rate': 1,            
            'revisit_prob': 0.33,         
            'num_topics': 5,
            'message_size_range': (MESSAGE_SIZE_MIN, MESSAGE_SIZE_MAX),
            'topic_access_weights': skewed_weights,  # SKEWED              
        }
    )

    # Experiment 7: Time-based Retention (10 MB storage, 3400 retention steps)
    experiment_7 = run_experiment(
        name="Time-based Retention (skewed, 10 MB storage, 3400 retention steps)",
        broker_config={
            'total_storage': 10_000_000,   # 10 MB
            'num_partitions_per_topic': 3,
            'retention_mode': 'time',
            'retention_steps': 3400,   
        },
        sim_config={
            'duration': 10_000,    
            'publish_rate': 1,      
            'revisit_prob': 0.33,    
            'num_topics': 5,
            'message_size_range': (MESSAGE_SIZE_MIN, MESSAGE_SIZE_MAX),
            'topic_access_weights': skewed_weights,  # SKEWED
        }
    )
    
    # Experiment 8: Lossy-priority Retention (10 MB storage, 80% Utilization)
    experiment_8 = run_experiment(
        name="Lossy-priority Retention (skewed, 10 MB storage, 80% Utilization)",
        broker_config={
            'total_storage': 10_000_000,   # 10 MB total
            'num_partitions_per_topic': 3,
            'retention_mode': 'lossy_priority',
            'capacity_byte': 8_000_000,    # 8 MB capacity
            'eviction_batch_size': 20,
        },
        sim_config={
            'duration': 10_000,
            'publish_rate': 1,            
            'revisit_prob': 0.33,         
            'num_topics': 5,
            'message_size_range': (MESSAGE_SIZE_MIN, MESSAGE_SIZE_MAX),
            'topic_access_weights': skewed_weights,  # SKEWED              
        }
    )

    # Experiment 9: Time-based Retention (10 MB storage, 4000 retention steps)
    experiment_9 = run_experiment(
        name="Time-based Retention (skewed, 10 MB storage, 4000 retention steps)",
        broker_config={
            'total_storage': 10_000_000,   # 10 MB
            'num_partitions_per_topic': 3,
            'retention_mode': 'time',
            'retention_steps': 4000,   
        },
        sim_config={
            'duration': 10_000,    
            'publish_rate': 1,      
            'revisit_prob': 0.33,    
            'num_topics': 5,
            'message_size_range': (MESSAGE_SIZE_MIN, MESSAGE_SIZE_MAX),
            'topic_access_weights': skewed_weights,  # SKEWED
        }
    )
    
    # Experiment 10: Lossy-priority Retention (10 MB storage, 100% Utilization)
    experiment_10 = run_experiment(
        name="Lossy-priority Retention (skewed, 10 MB storage, 100% Utilization)",
        broker_config={
            'total_storage': 10_000_000,   # 10 MB total
            'num_partitions_per_topic': 3,
            'retention_mode': 'lossy_priority',
            'capacity_byte': 10_000_000,    # 10 MB capacity
            'eviction_batch_size': 20,
        },
        sim_config={
            'duration': 10_000,
            'publish_rate': 1,            
            'revisit_prob': 0.33,         
            'num_topics': 5,
            'message_size_range': (MESSAGE_SIZE_MIN, MESSAGE_SIZE_MAX),
            'topic_access_weights': skewed_weights,  # SKEWED              
        }
    )
    
    # Summary comparison
    print("\n\n" + "="*100)
    print("EXPERIMENT SUMMARY")
    print("="*100)
    print(f"{'Experiment':<60} {'Hit Rate':>10} {'Misses':>8} {'Published':>10}")
    print("-"*100)
    
    experiments = [
        ("Time-based Retention (skewed, 10 MB storage, 850 retention steps)", experiment_1),
        ("Lossy-priority Retention (skewed, 10 MB storage, 20% Utilization)", experiment_2),
        ("Time-based Retention (skewed, 10 MB storage, 1650 retention steps)", experiment_3),
        ("Lossy-priority Retention (skewed, 10 MB storage, 40% Utilization)", experiment_4),
        ("Time-based Retention (skewed, 10 MB storage, 2500 retention steps)", experiment_5),
        ("Lossy-priority Retention (skewed, 10 MB storage, 60% Utilization)", experiment_6),
        ("Time-based Retention (skewed, 10 MB storage, 3400 retention steps)", experiment_7),
        ("Lossy-priority Retention (skewed, 10 MB storage, 80% Utilization)", experiment_8),
        ("Time-based Retention (skewed, 10 MB storage, 4000 retention steps)", experiment_9),
        ("Lossy-priority Retention (skewed, 10 MB storage, 100% Utilization)", experiment_10),
    ]
    
    for name, metrics in experiments:
        hit_rate = metrics['hit_rate'] * 100
        misses = metrics['cache_misses']
        published = metrics['total_published']
        print(f"{name:<60} {hit_rate:>9.1f}% {misses:>8} {published:>10}")

if __name__ == "__main__":
    main()