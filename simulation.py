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
    ):
        self.broker = broker
        self.duration = duration
        self.publish_rate = publish_rate
        self.revisit_prob = revisit_prob
        self.num_topics = num_topics
        
        # Track published messages for revisits
        self.published_messages = []
        
        # Metrics
        self.total_published = 0
        self.cache_hits = 0
        self.cache_misses = 0
        self.storage_samples = []
    
    def run(self):
        """Run the simulation"""
        print(f"\n{'='*70}")
        print(f"Starting Simulation:")
        print(f"  Duration: {self.duration} time steps")
        print(f"  Publish rate: every {self.publish_rate} steps")
        print(f"  Revisit probability: {self.revisit_prob}")
        print(f"  Topics: {self.num_topics}")
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
        
        # Publish message
        result = self.broker.publishMsg(
            topic_id=topic_id,
            payload_size=random.randint(500, 2000),
            timestamp=time_step,
            producer_id="producer-1",
            key=None  # Random partition assignment
        )
        
        if result['success']:
            self.total_published += 1
            # Save for potential revisit
            self.published_messages.append({
                'topic_id': topic_id,
                'partition': result['partition'],
                'offset': result['offset'],
                'time': time_step
            })
    
    def consume_messages(self, time_step):
        """Randomly revisit old messages"""
        if not self.published_messages:
            return
        
        # Revisit with some probability
        if random.random() < self.revisit_prob:
            # Pick random old message
            old_msg = random.choice(self.published_messages)
            
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
    Scale: 1:2000 of LinkedIn's original deployment (2011 paper)
    
    Broker: 500 MB (represents 1 TB production broker)
    Duration: 2100 time steps (represents 7 days)
    Messages: ~350K total (represents ~100M/day at production scale)
    Message size: 500-2000 bytes (1 KB average)
    """
    
    # ============================================
    # CONFIGURATION
    # ============================================
    
    print("\n" + "="*70)
    print("RUNNING EXPERIMENTS")
    print("="*70)
    
    # # Experiment 1: Time-based Retention (10 MB storage, 1000 retention steps)
    # experiment_1 = run_experiment(
    #     name="Time-based Retention (10 MB storage, 1000 retention steps)",
    #     broker_config={
    #         'total_storage': 10_000_000,   # 10 MB
    #         'num_partitions_per_topic': 3,
    #         'retention_mode': 'time',
    #         'retention_steps': 1000,   
    #     },
    #     sim_config={
    #         'duration': 10_000,    
    #         'publish_rate': 1,      
    #         'revisit_prob': 0.33,    
    #         'num_topics': 5,
    #     }
    # )

    # # Experiment 2: Time-based Retention (10 MB storage, 2000 retention steps)
    # experiment_2 = run_experiment(
    #     name="Time-based Retention (10 MB storage, 2000 retention steps)",
    #     broker_config={
    #         'total_storage': 10_000_000,   # 10 MB
    #         'num_partitions_per_topic': 3,
    #         'retention_mode': 'time',
    #         'retention_steps': 2000,   
    #     },
    #     sim_config={
    #         'duration': 10_000,    
    #         'publish_rate': 1,      
    #         'revisit_prob': 0.33,    
    #         'num_topics': 5,
    #     }
    # )

    # Experiment 3: Time-based Retention (10 MB storage, 3400 retention steps)
    experiment_3 = run_experiment(
        name="Time-based Retention (10 MB storage, 3400 retention steps)",
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
        }
    )
    
    # Experiment 4: Lossy-priority Retention (10 MB storage, 80% Utilization)
    experiment_4 = run_experiment(
        name="Lossy-priority Retention (10 MB storage, 80% Utilization)",
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
        }
    )
    
    # # Experiment 4: Very limited storage (extreme lossy)
    # experiment_4 = run_experiment(
    #     name="Very Limited Storage (500 KB capacity)",
    #     broker_config={
    #         'total_storage': 1_000_000,   # 1 MB total
    #         'num_partitions_per_topic': 3,
    #         'retention_mode': 'lossy_priority',
    #         'capacity_byte': 500_000,     # VERY AGGRESSIVE: 500 KB capacity
    #         'eviction_batch_size': 20,    # Evict 20 at a time
    #     },
    #     sim_config={
    #         'duration': 10000,
    #         'publish_rate': 2,            # Very high production
    #         'revisit_prob': 0.3,          # High revisit
    #         'num_topics': 5,
    #     }
    # )
    
    # # Experiment 5: High load with moderate retention
    # experiment_5 = run_experiment(
    #     name="High Load + Moderate Retention",
    #     broker_config={
    #         'total_storage': 3_000_000,   # 3 MB
    #         'num_partitions_per_topic': 3,
    #         'retention_mode': 'time',
    #         'retention_steps': 1000,      # Moderate retention
    #     },
    #     sim_config={
    #         'duration': 15000,            # Very long run
    #         'publish_rate': 2,            # Very high production
    #         'revisit_prob': 0.15,         # Moderate revisit
    #         'num_topics': 4,
    #     }
    # )
    
    # Summary comparison
    print("\n\n" + "="*100)
    print("EXPERIMENT SUMMARY")
    print("="*100)
    print(f"{'Experiment':<60} {'Hit Rate':>10} {'Misses':>8} {'Published':>10}")
    print("-"*100)
    
    experiments = [
        # ("Time-based Retention (10 MB storage, 1000 retention steps)", experiment_1),
        # ("Time-based Retention (10 MB storage, 2000 retention steps)", experiment_2),
        ("Time-based Retention (10 MB storage, 3400 retention steps)", experiment_3),
        ("Lossy-priority Retention (10 MB storage, 80% Utilization)", experiment_4),
        # ("Very Limited Storage (500 KB)", experiment_4),
        # ("High Load + Moderate Retention", experiment_5),
    ]
    
    for name, metrics in experiments:
        hit_rate = metrics['hit_rate'] * 100
        misses = metrics['cache_misses']
        published = metrics['total_published']
        print(f"{name:<60} {hit_rate:>9.1f}% {misses:>8} {published:>10}")
    
    # print("="*70)
    # print("\nKey Insights:")
    # print("- Lower hit rate = More aggressive retention (more deletions)")
    # print("- Cache misses = Messages deleted before they could be revisited")
    # print("- Compare different retention strategies and their trade-offs")
    # print("="*70)


if __name__ == "__main__":
    main()