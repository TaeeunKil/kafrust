import unittest

from scripts.wait_for_kafka_topic import ready_topic


class TopicReadinessTests(unittest.TestCase):
    def test_complete_replication(self):
        self.assertTrue(ready_topic("Topic: a Partition: 0 Leader: 2 Replicas: 1,2,3 Isr: 3,1,2", 1, 3))

    def test_missing_partition_or_leader(self):
        self.assertFalse(ready_topic("Topic: a PartitionCount: 1 ReplicationFactor: 3", 1, 3))
        self.assertFalse(ready_topic("Partition: 0 Leader: -1 Replicas: 1,2,3 Isr: 1,2,3", 1, 3))

    def test_replication_still_catching_up(self):
        self.assertFalse(ready_topic("Partition: 0 Leader: 1 Replicas: 1,2,3 Isr: 1", 1, 3))
        self.assertFalse(ready_topic("Partition: 0 Leader: 1 Replicas: 1,2,3 Isr: ", 1, 3))

    def test_duplicate_partition_cannot_hide_missing_partition(self):
        row = "Partition: 0 Leader: 1 Replicas: 1,2,3 Isr: 1,2,3\n"
        self.assertFalse(ready_topic(row * 2, 2, 3))


if __name__ == "__main__":
    unittest.main()
