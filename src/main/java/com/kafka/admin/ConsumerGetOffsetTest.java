package com.kafka.admin;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

public class ConsumerGetOffsetTest {
    static final Logger LOG = LoggerFactory.getLogger(ConsumerGetOffsetTest.class);

    @Test
    public void consumeGetAllPartitions() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.216.86:9092");
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        String topic = "first";
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
            for (int i = 0; i < partitionInfos.size(); i++) {
                PartitionInfo partitionInfo = partitionInfos.get(i);
                System.out.println(partitionInfo);
            }

        }

    }

    @Test
    public void consumeNoGroupIdError() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.216.86:9092");
        // enable.auto.commit cannot be set to true when default group id (null) is used.
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        String topic = "first";
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            // To use the group management or offset commit APIs, you must provide a valid group.id in the consumer configuration.
            consumer.subscribe(Collections.singletonList(topic));
            int cnt = 0;
            long start = System.currentTimeMillis();
            while (cnt < 100 && (System.currentTimeMillis() - start) < 3000) {
                ConsumerRecords<String, String> records =  consumer.poll(Duration.ofMillis(250));
                for (ConsumerRecord<String, String> record : records) {
                    cnt++;
                    LOG.warn("record offset = {}, timestamp = {}, key = {}", record.offset(), new java.sql.Timestamp(record.timestamp()), record.key());
                    LOG.warn("record value = {}", record.value());
                }
            }
        }
    }

    /**
     * 没有消费者组，必须使用assign消费
     * kafka有两种消费方式：
     *    1. sub 订阅模式，必须设置消费者组。主动订阅的情况下，消费者协调器会协调消费者进行分区消费，有一个负载均衡的理念在里面。
     *    2. assign 手动指定分区模式。手动指定分区进行消费的话，就会失去组的特性
     *
     */
    @Test
    public void consumeNoGroupId() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.216.86:9092");
        //props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        props.put("max.poll.records", "200"); // 默认一次最多拉取500条
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        String topic = "first";
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
            List<TopicPartition> partitions = new ArrayList<>(partitionInfos.size());
            for (int i = 0; i < partitionInfos.size(); i++) {
                PartitionInfo partitionInfo = partitionInfos.get(i);
                partitions.add(new TopicPartition(topic, partitionInfo.partition()));
            }
            consumer.assign(partitions);

            int cnt = 0;
            long start = System.currentTimeMillis();
            while (cnt < 2000 && (System.currentTimeMillis() - start) < 3000) {
                ConsumerRecords<String, String> records =  consumer.poll(Duration.ofMillis(250));
                Map<Integer, Long> offsets = new HashMap<>();
                for (ConsumerRecord<String, String> record : records) {
                    cnt++;
                    LOG.warn("record partition = {}, offset = {}, timestamp = {}, key = {}", record.partition(), record.offset(),
                            new java.sql.Timestamp(record.timestamp()).toString(), record.key());
                    LOG.warn("record value = {}", record.value());
                    offsets.put(record.partition(), record.offset() + 1);
                }
                if(!offsets.isEmpty()){
                    LOG.warn("next offsets = {}", offsets);
                }
            }
        }
    }

    @Test
    public void consumeNoGroupIdSeed() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.216.86:9092");
        //props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        props.put("max.poll.records", "200"); // 默认一次最多拉取500条
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        String topic = "first";
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
            List<TopicPartition> partitions = new ArrayList<>(partitionInfos.size());
            for (int i = 0; i < partitionInfos.size(); i++) {
                PartitionInfo partitionInfo = partitionInfos.get(i);
                partitions.add(new TopicPartition(topic, partitionInfo.partition()));
            }
            consumer.assign(partitions);
            for (TopicPartition partition : partitions) {
                consumer.seek(partition, 100);
            }

            int cnt = 0;
            long start = System.currentTimeMillis();
            while (cnt < 2000 && (System.currentTimeMillis() - start) < 3000) {
                ConsumerRecords<String, String> records =  consumer.poll(Duration.ofMillis(250));
                Map<Integer, Long> offsets = new HashMap<>();
                for (ConsumerRecord<String, String> record : records) {
                    cnt++;
                    LOG.warn("record partition = {}, offset = {}, timestamp = {}, key = {}", record.partition(), record.offset(),
                            new java.sql.Timestamp(record.timestamp()).toString(), record.key());
                    LOG.warn("record value = {}", record.value());
                    offsets.put(record.partition(), record.offset() + 1);
                }
                if(!offsets.isEmpty()){
                    LOG.warn("next offsets = {}", offsets);
                }
            }
        }
    }

    @Test
    public void consumeAssignCommitOffsetsSync() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.216.86:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        props.put("max.poll.records", "100"); // 默认一次最多拉取500条
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        String topic = "first";
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
            List<TopicPartition> partitions = new ArrayList<>(partitionInfos.size());
            for (int i = 0; i < partitionInfos.size(); i++) {
                PartitionInfo partitionInfo = partitionInfos.get(i);
                partitions.add(new TopicPartition(topic, partitionInfo.partition()));
            }
            consumer.assign(partitions);

            int cnt = 0;
            long start = System.currentTimeMillis();
            while (cnt < 1000 && (System.currentTimeMillis() - start) < 3000) {
                ConsumerRecords<String, String> records =  consumer.poll(Duration.ofMillis(250));
                Map<Integer, Long> offsets = new HashMap<>();
                for (ConsumerRecord<String, String> record : records) {
                    cnt++;
                    LOG.warn("record partition = {}, offset = {}, timestamp = {}, key = {}", record.partition(), record.offset(),
                            new java.sql.Timestamp(record.timestamp()).toString(), record.key());
                    LOG.warn("record value = {}", record.value());
                    offsets.put(record.partition(), record.offset() + 1);
                }

                Map<TopicPartition, OffsetAndMetadata> submitOffsets = new HashMap<>();
                offsets.forEach((partition, offset) -> {
                    submitOffsets.put(new TopicPartition(topic, partition),
                            new OffsetAndMetadata(offset));

                    // position 返回的就是下次要拉取的offset， 实际情况poll一批数据处理后可以使用这个position提交
                    // 当上面的break注释后position和offset就是相等的
                    long position = consumer.position(new TopicPartition(topic, partition));
                    LOG.warn("next position = {}, pre submit offset = {}", position, offset);
                });

                consumer.commitSync(submitOffsets); //　提交偏移量
            }
        }
    }

    @Test
    public void consumeGetLatestOffsets() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.216.86:9092");
        //props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        String topic = "first";
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
            List<TopicPartition> partitions = new ArrayList<>(partitionInfos.size());
            for (int i = 0; i < partitionInfos.size(); i++) {
                PartitionInfo partitionInfo = partitionInfos.get(i);
                partitions.add(new TopicPartition(topic, partitionInfo.partition()));
            }

            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);

            consumer.assign(partitions);
            consumer.seekToEnd(partitions);

            Map<Integer, Long> offsets = new HashMap<>();
            for (TopicPartition p : partitions) {
                offsets.put(p.partition(), consumer.position(p));
            }

            System.out.println(endOffsets);
            System.out.println(offsets);

            endOffsets.forEach((partition, offset) -> {
                assert offsets.get(partition.partition()).equals(offset);
            });
        }
    }

    @Test
    public void consumeGetEarliestOffsets() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.216.86:9092");
        //props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        String topic = "first";
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
            List<TopicPartition> partitions = new ArrayList<>(partitionInfos.size());
            for (int i = 0; i < partitionInfos.size(); i++) {
                PartitionInfo partitionInfo = partitionInfos.get(i);
                partitions.add(new TopicPartition(topic, partitionInfo.partition()));
            }

            Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(partitions);

            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);

            Map<Integer, Long> offsets = new HashMap<>();
            for (TopicPartition p : partitions) {
                offsets.put(p.partition(), consumer.position(p));
            }

            System.out.println(beginningOffsets);
            System.out.println(offsets);

            beginningOffsets.forEach((partition, offset) -> {
                assert offsets.get(partition.partition()).equals(offset);
            });
        }
    }

    @Test
    public void consumeGetTimestampOffsets() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.216.86:9092");
        //props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        String topic = "first";
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
            List<TopicPartition> partitions = new ArrayList<>(partitionInfos.size());
            for (int i = 0; i < partitionInfos.size(); i++) {
                PartitionInfo partitionInfo = partitionInfos.get(i);
                partitions.add(new TopicPartition(topic, partitionInfo.partition()));
            }

            Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
            long ts = System.currentTimeMillis() - 1000 * 60 * 60 * 100;
            for (TopicPartition p : partitions) {
                timestampsToSearch.put(p, ts);
            }

            // 第一个大于这个时间戳的offset，如果时间戳大于最新offset的时间则返回null
            Map<TopicPartition, OffsetAndTimestamp> partitionOffset = consumer.offsetsForTimes(timestampsToSearch);
            partitionOffset.forEach((k, v)->{
                System.out.println(k + " -> " + v);
            });

        }
    }

    @Test
    public void consumeGetGroupOffsets() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.216.86:9092");
        props.put("group.id", "test");
        //props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "latest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        String topic = "first";
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
            List<TopicPartition> partitions = new ArrayList<>(partitionInfos.size());
            for (int i = 0; i < partitionInfos.size(); i++) {
                PartitionInfo partitionInfo = partitionInfos.get(i);
                partitions.add(new TopicPartition(topic, partitionInfo.partition()));
            }

            consumer.assign(partitions);

            Map<Integer, Long> offsets = new HashMap<>();
            for (TopicPartition p : partitions) {
                // 默认情况下，KafkaConsumer会自动查找提交组偏移量的consumer位置，因此我们不需要这样做。
                // 没有提交则按照auto.offset.reset里的配置获取
                offsets.put(p.partition(), consumer.position(p));
            }

            System.out.println(offsets);
        }
    }

    @Test
    public void consumeSubmitEarliestOffsets() throws Exception {
        Properties props = new Properties();
        props.put("group.id", "test");
        props.put("bootstrap.servers", "192.168.216.86:9092");
        //props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        String topic = "first";
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
            List<TopicPartition> partitions = new ArrayList<>(partitionInfos.size());
            for (int i = 0; i < partitionInfos.size(); i++) {
                PartitionInfo partitionInfo = partitionInfos.get(i);
                partitions.add(new TopicPartition(topic, partitionInfo.partition()));
            }

            Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(partitions);

            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);

            Map<TopicPartition, OffsetAndMetadata> submitOffsets = new HashMap<>();
            for (TopicPartition p : partitions) {
                submitOffsets.put(p, new OffsetAndMetadata(consumer.position(p)));
            }

            System.out.println(beginningOffsets);
            System.out.println(submitOffsets);

            consumer.commitSync(submitOffsets); //　提交偏移量
        }
    }

    @Test
    public void consumeSubmitLatestOffsets() throws Exception {
        Properties props = new Properties();
        props.put("group.id", "test");
        props.put("bootstrap.servers", "192.168.216.86:9092");
        //props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        String topic = "first";
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
            List<TopicPartition> partitions = new ArrayList<>(partitionInfos.size());
            for (int i = 0; i < partitionInfos.size(); i++) {
                PartitionInfo partitionInfo = partitionInfos.get(i);
                partitions.add(new TopicPartition(topic, partitionInfo.partition()));
            }

            consumer.assign(partitions);
            consumer.seekToEnd(partitions);

            Map<TopicPartition, OffsetAndMetadata> submitOffsets = new HashMap<>();
            for (TopicPartition p : partitions) {
                submitOffsets.put(p, new OffsetAndMetadata(consumer.position(p)));
            }

            System.out.println(submitOffsets);

            consumer.commitSync(submitOffsets); //　提交偏移量
        }
    }
}
