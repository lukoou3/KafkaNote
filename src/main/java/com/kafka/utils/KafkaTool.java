package com.kafka.utils;

import com.utils.ArgsTool;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 java -cp kafka-note-jar-with-dependencies.jar com.kafka.utils.KafkaTool \
 --bootstrap 192.168.216.86:9092 --action show_topic_latest_offset --topic first


 java -cp kafka-note-jar-with-dependencies.jar com.kafka.utils.KafkaTool \
 --bootstrap 192.168.216.86:9092 --action reset_group_offset_latest --topic first --group_id test

 java -cp kafka-note-jar-with-dependencies.jar com.kafka.utils.KafkaTool \
 --bootstrap 192.168.216.86:9092 --action reset_group_offset_for_ts --topic first --group_id test --ts 0

 --

 java -cp kafka-note-jar-with-dependencies.jar com.kafka.utils.KafkaTool \
 --bootstrap 192.168.40.151:9092 --action show_topic_latest_offset --topic SESSION-RECORD-COMPLETED-TEST

 java -cp kafka-note-jar-with-dependencies.jar com.kafka.utils.KafkaTool \
 --bootstrap 192.168.40.151:9092 --action reset_group_offset_latest --topic SESSION-RECORD-COMPLETED-TEST --group_id test-group

 java -cp kafka-note-jar-with-dependencies.jar com.kafka.utils.KafkaTool \
 --bootstrap 192.168.40.151:9092 --action reset_group_offset_for_ts --topic SESSION-RECORD-COMPLETED-TEST --group_id test-group --ts 0

 java -cp kafka-note-jar-with-dependencies.jar com.kafka.utils.KafkaTool \
 --bootstrap 192.168.40.151:9092 --action reset_group_offset_for_ts --topic SESSION-RECORD-COMPLETED-TEST --group_id test-group --ts 1695702000000
 */
public class KafkaTool {

    /**
     * --bootstrap 192.168.216.86:9092 --action show_topic_latest_offset --topic first
     * --bootstrap 192.168.216.86:9092 --action reset_group_offset_latest --topic first --group_id test
     * --bootstrap 192.168.216.86:9092 --action reset_group_offset_for_ts --topic first --group_id test --ts 0
     */
    public static void main(String[] args) throws Exception {
        ArgsTool argsTool = ArgsTool.fromArgs(args);
        System.out.println(argsTool.getKwargs());

        final String bootstrap = argsTool.getRequired("bootstrap");
        final String action = argsTool.getRequired("action");

        if("create_topic".equals(action)){
            final String topic = argsTool.getRequired("topic");
            final int partitions = argsTool.getInt("partitions", 1);
            final int replication = argsTool.getInt("replication", 1);
            createTopic(bootstrap, topic, partitions, replication);
        } else if ("delete_topic".equals(action)) {
            final String topic = argsTool.getRequired("topic");
            deleteTopic(bootstrap, topic);
        } else if ("show_topic_latest_offset".equals(action)) {
            final String topic = argsTool.getRequired("topic");
            showTopicPartitionLatestOffset(bootstrap, topic);
        } else if ("reset_group_offset_latest".equals(action)) {
            final String topic = argsTool.getRequired("topic");
            final String groupId = argsTool.getRequired("group_id");
            resetConsumerGroupOffsetLatest(bootstrap, topic, groupId);
        } else if ("reset_group_offset_for_ts".equals(action)) {
            final String topic = argsTool.getRequired("topic");
            final String groupId = argsTool.getRequired("group_id");
            final long ts = argsTool.getLongRequired("ts");
            resetConsumerGroupOffsetForTimestamp(bootstrap, topic, groupId, ts);
        } else{
            throw new RuntimeException(action);
        }
    }

    public static void createTopic(String bootstrap, String topic, int numPartitions, int replicationFactor) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        AdminClient client = AdminClient.create(props);
        NewTopic newTopic = new NewTopic(topic, numPartitions, (short) replicationFactor);
        CreateTopicsResult result = client.createTopics(Collections.singletonList(newTopic));
        Map<String, KafkaFuture<Void>> values = result.values();
        values.get(topic).get();
        System.out.println("create topic " + topic + "success");
    }

    public static void deleteTopic(String bootstrap, String topic) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        AdminClient client = AdminClient.create(props);
        DeleteTopicsResult result = client.deleteTopics(Collections.singletonList(topic));
        result.all().get();
        System.out.println("delete topic " + topic + "success");
    }

    public static void showTopicPartitionLatestOffset(String bootstrap, String topic) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
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
            String info = String.format("topic:%s, partition:%d, latest offset:%d", topicPartition.topic(), topicPartition.partition(), offsetInfo.offset());
            System.out.println(info);
        });
    }

    public static void resetConsumerGroupOffsetLatest(String bootstrap, String topic, String groupId) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
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
            String info = String.format("topic:%s, partition:%d, latest offset:%d", topicPartition.topic(), topicPartition.partition(), offsetInfo.offset());
            System.out.println(info);
        });

        // 提交offset
        AlterConsumerGroupOffsetsResult submitOffsetsResult = client.alterConsumerGroupOffsets(groupId, submitOffsets);
        submitOffsetsResult.all().get();
        System.out.println("reset topic " + topic + " groupId " +  groupId + "success");
    }

    public static void resetConsumerGroupOffsetForTimestamp(String bootstrap, String topic, String groupId, long timestamp) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        AdminClient client = AdminClient.create(props);

        // 获取分区
        DescribeTopicsResult result = client.describeTopics(Collections.singletonList(topic));
        Map<String, TopicDescription> descriptionMap = result.all().get();
        int[] partitions = descriptionMap.get(topic).partitions().stream().mapToInt(x -> x.partition()).toArray();

        Map<TopicPartition, OffsetSpec> topicPartitionOffsets = new HashMap<>();
        for (int i = 0; i < partitions.length; i++) {
            topicPartitionOffsets.put(new TopicPartition(topic, partitions[i]), OffsetSpec.forTimestamp(timestamp));
        }
        ListOffsetsResult offsetsResult = client.listOffsets(topicPartitionOffsets);

        Map<Integer, Long> tsOffsets = new HashMap<>();
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> offsetsResultInfoMap = offsetsResult.all().get();
        // 没有大于这个ts的消息，返回的offset=-1
        Map<TopicPartition, OffsetSpec> latestOffsetSpec = new HashMap<>();
        for (Map.Entry<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> entry : offsetsResultInfoMap.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            ListOffsetsResult.ListOffsetsResultInfo offsetInfo = entry.getValue();
            int partition = topicPartition.partition();
            long offset = offsetInfo.offset();
            tsOffsets.put(partition, offset);
            if(offset < 0){
                latestOffsetSpec.put(topicPartition, OffsetSpec.latest());
                String info = String.format("topic:%s, partition:%d can not get ts, reset to latest", topicPartition.topic(), topicPartition.partition());
                System.out.println(info);
            }else{
                String info = String.format("topic:%s, partition:%d, offset:%d, ts:%d, timestamp:%s", topicPartition.topic(), topicPartition.partition(),
                        offsetInfo.offset(), offsetInfo.timestamp(), new java.sql.Timestamp(offsetInfo.timestamp()).toString());
                System.out.println(info);
            }
        }

        if(!latestOffsetSpec.isEmpty()){
            offsetsResult = client.listOffsets(latestOffsetSpec);
            offsetsResultInfoMap = offsetsResult.all().get();
            offsetsResultInfoMap.forEach((topicPartition, offsetInfo)->{
                int partition = topicPartition.partition();
                long offset = offsetInfo.offset();
                tsOffsets.put(partition, offset);
                String info = String.format("topic:%s, partition:%d, latest offset:%d", topicPartition.topic(), topicPartition.partition(), offsetInfo.offset());
                System.out.println(info);
            });
        }

        Map<TopicPartition, OffsetAndMetadata> submitOffsets = new HashMap<>();
        tsOffsets.forEach((partition, offset)->{
            submitOffsets.put(new TopicPartition(topic, partition),
                    new OffsetAndMetadata(offset));
            assert offset >= 0;
        });

        // 提交offset
        AlterConsumerGroupOffsetsResult submitOffsetsResult = client.alterConsumerGroupOffsets(groupId, submitOffsets);
        submitOffsetsResult.all().get();
        System.out.println("reset topic " + topic + " groupId " +  groupId + "success");
    }
}
