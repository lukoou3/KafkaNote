package com.kafka.utils;

import org.junit.Test;

public class KafkaToolTest {

    @Test
    public void showTopicPartitionLatestOffset() throws Exception {
        KafkaTool.showTopicPartitionLatestOffset("192.168.216.86:9092", "first");
    }

    @Test
    public void resetConsumerGroupOffsetLatest() throws Exception {
        KafkaTool.resetConsumerGroupOffsetLatest("192.168.216.86:9092", "first", "test");
    }

    @Test
    public void resetConsumerGroupOffsetForTimestamp() throws Exception {
        //KafkaTool.resetConsumerGroupOffsetForTimestamp("192.168.216.86:9092", "first", "test", 0L);
        KafkaTool.resetConsumerGroupOffsetForTimestamp("192.168.216.86:9092", "first", "test", System.currentTimeMillis());
    }
}
