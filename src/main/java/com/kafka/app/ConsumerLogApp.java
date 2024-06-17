package com.kafka.app;

import com.alibaba.fastjson2.JSON;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;
import com.kafka.faker.Preconditions;
import com.utils.ArgsTool;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class ConsumerLogApp {

    /**
     * --bootstrap 192.168.44.12:9092 --topic OBJECT-STATISTICS-METRIC --seekToTime "2024-06-14 17:00:00" --maxOutCount 10
     * --bootstrap 192.168.44.12:9092 --topic OBJECT-STATISTICS-METRIC --seekToTime "2024-06-14 17:00:00" --filterMsgExpr "tags.object_id == 50"
     *
     */
    public static void main(String[] args) throws Exception {
        ArgsTool argsTool = ArgsTool.fromArgs(args);
        System.out.println(argsTool.getKwargs());

        final int parallelism = argsTool.getInt("parallelism", 1);
        final String bootstrap = argsTool.getRequired("bootstrap");
        final String topic = argsTool.getRequired("topic");
        final String autoOffsetReset = argsTool.get("autoOffsetReset", "latest");
        final String filterMsgExpr = argsTool.get("filterMsgExpr");
        final boolean seekToBeginning = argsTool.getInt("seekToBeginning", 0) == 1;
        final boolean seekToEnd = argsTool.getInt("seekToEnd", 0) == 1;
        String seekToTime = argsTool.get("seekToTime");
        long seekToTimestamp = StringUtils.isBlank(seekToTime)? 0: new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(seekToTime).getTime();
        String maxTime = argsTool.get("maxTime");
        long maxTimestamp = StringUtils.isBlank(maxTime)? Long.MAX_VALUE: new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(maxTime).getTime();
        final long maxOutCount = argsTool.getLong("maxOutCount", 100000);

        AtomicLong outCount = new AtomicLong();
        List<List<Integer>> subPartitionsList = splitPartitions(bootstrap, topic, null, parallelism);

        Thread[] threads = new Thread[parallelism];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = getConsumerThread("Thread:" + (i + 1) + "/" + parallelism, bootstrap, topic, subPartitionsList.get(i),
                    autoOffsetReset, filterMsgExpr, seekToBeginning, seekToEnd, seekToTimestamp, maxTimestamp, maxOutCount, outCount);
        }

        for (int i = 0; i < threads.length; i++) {
            threads[i].start();
        }

        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
        }

    }

    private static Thread getConsumerThread(String name, String bootstrap, String topic, List<Integer> subPartitions, String autoOffsetReset, String filterMsgExpr,
                                            boolean seekToBeginning, boolean seekToEnd, long seekToTimestamp, long maxTimestamp, long maxOutCount, AtomicLong outCount) {
        return new Thread(name) {
            @Override
            public void run() {
                Map<Integer, Boolean> partitionEnds = subPartitions.stream().collect(Collectors.toMap(x -> x, x -> false));
                Expression filterScript = StringUtils.isBlank(filterMsgExpr)?null:AviatorEvaluator.compile(filterMsgExpr);

                try (KafkaConsumer<String, String> consumer = getConsumer(bootstrap)) {
                    seedOffsets(consumer, topic, subPartitions, autoOffsetReset, seekToBeginning, seekToEnd, seekToTimestamp);
                    Map<String, Object> value;
                    while (true){
                        ConsumerRecords<String, String> records =  consumer.poll(Duration.ofMillis(250));
                        for (ConsumerRecord<String, String> record : records) {
                            if(record.timestamp() > maxTimestamp){
                                partitionEnds.put(record.partition(), true);
                                continue;
                            }
                            if(filterScript != null){
                                value = JSON.parseObject(record.value());
                                if(!(Boolean) filterScript.execute(value)){
                                    continue;
                                }
                            }
                            System.out.println(record.value());
                            if(outCount.addAndGet(1) >= maxOutCount){
                                return;
                            }
                        }

                        if(partitionEnds.values().stream().allMatch(x -> x)){
                            return;
                        }
                    }
                }
            }
        };
    }

    private static void seedOffsets(KafkaConsumer<String, String> consumer, String topic, List<Integer> subPartitions, String autoOffsetReset, boolean seekToBeginning, boolean seekToEnd, long seekToTimestamp){
        List<TopicPartition> partitions = new ArrayList<>(subPartitions.size());
        for (int i = 0; i < subPartitions.size(); i++) {
            partitions.add(new TopicPartition(topic, subPartitions.get(i)));
        }
        consumer.assign(partitions);
        if (seekToBeginning) {
            consumer.seekToBeginning(partitions);
        } else if (seekToEnd) {
            consumer.seekToEnd(partitions);
        } else if (seekToTimestamp > 0) {
            Map<TopicPartition, OffsetAndTimestamp> partitionOffsets = consumer.offsetsForTimes(partitions.stream().collect(Collectors.toMap(x -> x, x -> seekToTimestamp)));
            Map<TopicPartition, Long> defaultOffsets = "earliest".equals(autoOffsetReset) ? consumer.beginningOffsets(partitions) : consumer.endOffsets(partitions);
            for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : partitionOffsets.entrySet()) {
                if (entry.getValue() != null) {
                    consumer.seek(entry.getKey(), entry.getValue().offset());
                } else {
                    consumer.seek(entry.getKey(), defaultOffsets.get(entry.getKey()));
                }
            }
        }
    }

    private static List<List<Integer>> splitPartitions(String bootstrap, String topic, List<Integer> partitionIds, int parallelism) {
        try (KafkaConsumer<String, String> consumer = getConsumer(bootstrap)) {
            List<PartitionInfo> allPartitionInfos = consumer.partitionsFor(topic);
            List<Integer> partitions = new ArrayList<>(allPartitionInfos.size());
            for (int i = 0; i < allPartitionInfos.size(); i++) {
                PartitionInfo partitionInfo = allPartitionInfos.get(i);
                if (partitionIds != null && partitionIds.size() > 0) {
                    if (partitionIds.contains(partitionInfo.partition())) {
                        partitions.add(partitionInfo.partition());
                    }
                } else {
                    partitions.add(partitionInfo.partition());
                }
            }

            Preconditions.checkArgument(partitions.size() >= parallelism);

            List<List<Integer>> subPartitionsList = new ArrayList<>(parallelism);

            int sizePerSubtask = partitions.size() / parallelism;
            int moreSubtask = partitions.size() % parallelism;
            int index = 0;
            for (int i = 0; i < parallelism; i++) {
                List<Integer> subPartitions = new ArrayList<>();
                subPartitionsList.add(subPartitions);
                if (moreSubtask > i) {
                    for (int j = 0; j < sizePerSubtask + 1; j++) {
                        subPartitions.add(partitions.get(index++));
                    }
                } else {
                    for (int j = 0; j < sizePerSubtask; j++) {
                        subPartitions.add(partitions.get(index++));
                    }
                }
            }

            return subPartitionsList;
        }
    }

    private static KafkaConsumer<String, String> getConsumer(final String bootstrap) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "latest");
        props.put("kafka.session.timeout.ms", "60000");
        props.put("max.poll.records", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        return consumer;
    }
}
