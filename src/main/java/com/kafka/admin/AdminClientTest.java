package com.kafka.admin;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

public class AdminClientTest {
    static final Logger LOG = LoggerFactory.getLogger(AdminClientTest.class);

    static{
        assert CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG.equals("bootstrap.servers");
    }

    @Test
    public void listTopicNames() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.216.86:9092");
        AdminClient client = AdminClient.create(props);

        ListTopicsResult listTopicsResult = client.listTopics();
        Set<String> names = listTopicsResult.names().get();
        for (String name : names) {
            System.out.println(name);
        }

        client.close();
    }

    @Test
    public void listAllTopics() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.216.86:9092");
        AdminClient client = AdminClient.create(props);

        ListTopicsOptions options = new ListTopicsOptions();
        options.listInternal(true);
        ListTopicsResult listTopicsResult = client.listTopics(options);
        Map<String, TopicListing> nameToListings = listTopicsResult.namesToListings().get();
        for (Map.Entry<String, TopicListing> entry : nameToListings.entrySet()) {
            System.out.println(entry.getKey() + " -> " + entry.getValue());
        }

        client.close();
    }

    @Test
    public void createTopic() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.216.86:9092");
        AdminClient client = AdminClient.create(props);
        String topic = "session-record";
        topic = "first";
        // topic描述，2个分区1个副本，和命令行创建一样
        NewTopic newTopic = new NewTopic(topic, 2, (short) 1);
        CreateTopicsResult result = client.createTopics(Collections.singletonList(newTopic));
        Map<String, KafkaFuture<Void>> values = result.values();
        values.get(topic).get();
    }

    @Test
    public void deleteTopic() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.216.86:9092");
        AdminClient client = AdminClient.create(props);
        String topic = "session-record";
        topic = "first";
        DeleteTopicsResult result = client.deleteTopics(Collections.singletonList(topic));
        result.all().get();
    }

    @Test
    public void describeTopics() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.216.86:9092");
        AdminClient client = AdminClient.create(props);
        String topic = "session-record";
        DescribeTopicsResult result = client.describeTopics(Collections.singletonList(topic));
        Map<String, TopicDescription> descriptionMap = result.all().get();
        descriptionMap.forEach((k, v)->{
            System.out.println(k + " -> " + v);
        });

        System.out.println();
        System.out.println();

        descriptionMap.forEach((topicName, topicDescription)->{
            System.out.println("partition:" + topicName );
            List<TopicPartitionInfo> partitionInfos = topicDescription.partitions();
            for (TopicPartitionInfo partitionInfo : partitionInfos) {
                //System.out.println(partitionInfo.toString());
                System.out.println("partition:" + partitionInfo.partition());
            }
            System.out.println();
        });
    }

    @Test
    public void listGroupOffsets() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.216.86:9092");
        AdminClient client = AdminClient.create(props);
        ListConsumerGroupOffsetsResult result = client.listConsumerGroupOffsets("test");
        result.partitionsToOffsetAndMetadata().get().forEach((topicPartition, offsetAndMetadata)->{
            System.out.println(topicPartition.toString() + "->" + offsetAndMetadata);
        });
    }

    /**
     * earliest就是最早的offset，正常情况下是0
     * latest就是最新的offset，是下一条消息的offset
     * consumer group.id 提交的 offset就是consumer下次要消费的位置
     */
    @Test
    public void listOffsets_latest() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.216.86:9092");
        String topic = "first";
        AdminClient client = AdminClient.create(props);
        DescribeTopicsResult result = client.describeTopics(Collections.singletonList(topic));
        Map<String, TopicDescription> descriptionMap = result.all().get();
        int[] partitions = descriptionMap.get(topic).partitions().stream().mapToInt(x -> x.partition()).toArray();

        Map<TopicPartition, OffsetSpec> topicPartitionOffsets = new HashMap<>();
        for (int i = 0; i < partitions.length; i++) {
            topicPartitionOffsets.put(new TopicPartition(topic, partitions[i]), OffsetSpec.latest());
        }
        ListOffsetsResult offsetsResult = client.listOffsets(topicPartitionOffsets);
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> offsetsResultInfoMap = offsetsResult.all().get();
        offsetsResultInfoMap.forEach((topicPartition, offsetInfo)->{
            System.out.println(topicPartition.toString() + "->" + offsetInfo);
        });
    }

    @Test
    public void listOffsets_earliest() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.216.86:9092");
        String topic = "first";
        AdminClient client = AdminClient.create(props);
        DescribeTopicsResult result = client.describeTopics(Collections.singletonList(topic));
        Map<String, TopicDescription> descriptionMap = result.all().get();
        int[] partitions = descriptionMap.get(topic).partitions().stream().mapToInt(x -> x.partition()).toArray();

        Map<TopicPartition, OffsetSpec> topicPartitionOffsets = new HashMap<>();
        for (int i = 0; i < partitions.length; i++) {
            topicPartitionOffsets.put(new TopicPartition(topic, partitions[i]), OffsetSpec.earliest());
        }
        ListOffsetsResult offsetsResult = client.listOffsets(topicPartitionOffsets);
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> offsetsResultInfoMap = offsetsResult.all().get();
        offsetsResultInfoMap.forEach((topicPartition, offsetInfo)->{
            System.out.println(topicPartition.toString() + "->" + offsetInfo);
        });
    }

    @Test
    public void listOffsets_timestamp() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.216.86:9092");
        String topic = "first";
        AdminClient client = AdminClient.create(props);
        DescribeTopicsResult result = client.describeTopics(Collections.singletonList(topic));
        Map<String, TopicDescription> descriptionMap = result.all().get();
        int[] partitions = descriptionMap.get(topic).partitions().stream().mapToInt(x -> x.partition()).toArray();

        Map<TopicPartition, OffsetSpec> topicPartitionOffsets = new HashMap<>();
        //long ts = System.currentTimeMillis() - 1000 * 60 * 60 * 5;
        long ts = System.currentTimeMillis();
        for (int i = 0; i < partitions.length; i++) {
            topicPartitionOffsets.put(new TopicPartition(topic, partitions[i]), OffsetSpec.forTimestamp(ts));
        }
        ListOffsetsResult offsetsResult = client.listOffsets(topicPartitionOffsets);
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> offsetsResultInfoMap = offsetsResult.all().get();
        // 没有大于这个ts的消息，返回的offset=-1
        offsetsResultInfoMap.forEach((topicPartition, offsetInfo)->{
            System.out.println(topicPartition.toString() + "->" + offsetInfo);
        });
    }

    @Test
    public void alterConsumerGroupOffsets() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.216.86:9092");
        String topic = "first";
        AdminClient client = AdminClient.create(props);
        DescribeTopicsResult result = client.describeTopics(Collections.singletonList(topic));
        Map<String, TopicDescription> descriptionMap = result.all().get();
        int[] partitions = descriptionMap.get(topic).partitions().stream().mapToInt(x -> x.partition()).toArray();

        Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsets = new HashMap<>();
        for (int i = 0; i < partitions.length; i++) {
            topicPartitionOffsets.put(new TopicPartition(topic, partitions[i]), new OffsetAndMetadata(1));
        }

        AlterConsumerGroupOffsetsResult offsetsResult = client.alterConsumerGroupOffsets("test", topicPartitionOffsets);
        offsetsResult.all().get();
    }

    @Test
    public void alterConsumerGroupOffsets_earliest() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.216.86:9092");
        String topic = "first";
        AdminClient client = AdminClient.create(props);

        // 获取分区
        DescribeTopicsResult result = client.describeTopics(Collections.singletonList(topic));
        Map<String, TopicDescription> descriptionMap = result.all().get();
        int[] partitions = descriptionMap.get(topic).partitions().stream().mapToInt(x -> x.partition()).toArray();

        // 获取分区earliest offset
        Map<TopicPartition, OffsetSpec> topicPartitionOffsets = new HashMap<>();
        for (int i = 0; i < partitions.length; i++) {
            topicPartitionOffsets.put(new TopicPartition(topic, partitions[i]), OffsetSpec.earliest());
        }
        ListOffsetsResult offsetsResult = client.listOffsets(topicPartitionOffsets);
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> offsetsResultInfoMap = offsetsResult.all().get();

        Map<TopicPartition, OffsetAndMetadata> submitOffsets = new HashMap<>();
        offsetsResultInfoMap.forEach((topicPartition, offsetInfo)->{
            submitOffsets.put(new TopicPartition(topicPartition.topic(), topicPartition.partition()),
                    new OffsetAndMetadata(offsetInfo.offset()));
            LOG.warn("topic = {}, partition = {}, offset = {}", topicPartition.topic(), topicPartition.partition(), offsetInfo.offset());
        });

        // 提交offset
        AlterConsumerGroupOffsetsResult submitOffsetsResult = client.alterConsumerGroupOffsets("test", submitOffsets);
        submitOffsetsResult.all().get();
    }

    @Test
    public void alterConsumerGroupOffsets_latest() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.216.86:9092");
        String topic = "first";
        AdminClient client = AdminClient.create(props);

        // 获取分区
        DescribeTopicsResult result = client.describeTopics(Collections.singletonList(topic));
        Map<String, TopicDescription> descriptionMap = result.all().get();
        int[] partitions = descriptionMap.get(topic).partitions().stream().mapToInt(x -> x.partition()).toArray();

        // 获取分区latest offset
        Map<TopicPartition, OffsetSpec> topicPartitionOffsets = new HashMap<>();
        for (int i = 0; i < partitions.length; i++) {
            topicPartitionOffsets.put(new TopicPartition(topic, partitions[i]), OffsetSpec.latest());
        }
        ListOffsetsResult offsetsResult = client.listOffsets(topicPartitionOffsets);
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> offsetsResultInfoMap = offsetsResult.all().get();

        Map<TopicPartition, OffsetAndMetadata> submitOffsets = new HashMap<>();
        offsetsResultInfoMap.forEach((topicPartition, offsetInfo)->{
            submitOffsets.put(new TopicPartition(topicPartition.topic(), topicPartition.partition()),
                    new OffsetAndMetadata(offsetInfo.offset()));
            LOG.warn("topic = {}, partition = {}, offset = {}", topicPartition.topic(), topicPartition.partition(), offsetInfo.offset());
        });

        // 提交offset
        AlterConsumerGroupOffsetsResult submitOffsetsResult = client.alterConsumerGroupOffsets("test", submitOffsets);
        submitOffsetsResult.all().get();
    }

    /**
     * Offset就是consumer下次要消费的位置
     */
    @Test
    public void consume() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.216.86:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "earliest");
        props.put("max.poll.records", "100"); // 默认一次最多拉取500条
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList("first"));
            int cnt = 0;
            long start = System.currentTimeMillis();
            while (cnt < 100 && (System.currentTimeMillis() - start) < 3000) {
                ConsumerRecords<String, String> records =  consumer.poll(Duration.ofMillis(250));
                for (ConsumerRecord<String, String> record : records) {
                    cnt++;
                    LOG.warn("record partition = {}, offset = {}, timestamp = {}, key = {}", record.partition(), record.offset(), new java.sql.Timestamp(record.timestamp()), record.key());
                    LOG.warn("record value = {}", record.value());
                }
            }
        }
    }

    /**
     * Offset就是consumer下次要消费的位置，手动提交时就是提交拉取到每个分区最大offset + 1
     */
    @Test
    public void consumeCommitOffsetsSync() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.216.86:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        String topic = "first";

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));
            int cnt = 0;
            long start = System.currentTimeMillis();
            while (cnt < 1 && (System.currentTimeMillis() - start) < 3000) {
                ConsumerRecords<String, String> records =  consumer.poll(Duration.ofMillis(250));
                Map<Integer, Long> offsets = new HashMap<>();
                for (ConsumerRecord<String, String> record : records) {
                    cnt++;
                    LOG.warn("record offset = {}, timestamp = {}, key = {}", record.offset(), new java.sql.Timestamp(record.timestamp()), record.key());
                    LOG.warn("record value = {}", record.value());
                    offsets.put(record.partition(), record.offset() + 1);
                    if(cnt == 10){
                        break;
                    }
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

}
