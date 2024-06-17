package com.kafka.app;

import com.alibaba.fastjson2.JSON;
import com.kafka.faker.FakerUtils;
import com.kafka.faker.ObjectFaker;
import com.utils.ArgsTool;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;

public class FakerApp {

    // --bootstrap 192.168.44.12:9092 --topic first --fakerFilePath "D:\WorkSpace\KafkaNote\src\main\resources\mock_example.json" --batch 30
    public static void main(String[] args) throws Exception {
        ArgsTool argsTool = ArgsTool.fromArgs(args);
        System.out.println(argsTool.getKwargs());
        final String bootstrap = argsTool.getRequired("bootstrap");
        final String topic = argsTool.getRequired("topic");
        final String fakerFilePath = argsTool.getRequired("fakerFilePath");
        ObjectFaker faker = FakerUtils.parseObjectFakerFromJson(FileUtils.readFileToString(new File(fakerFilePath), StandardCharsets.UTF_8));
        final int speed = argsTool.getInt("speed", 1);
        final long batch = argsTool.getLong("batch", Long.MAX_VALUE);
        final int parallelism = argsTool.getInt("parallelism", 1);

        Thread[] threads = new Thread[parallelism];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = getProducerThread("Thread:" +(i + 1) + "/" + parallelism,
                    bootstrap, topic, speed, batch, faker, threads.length, i);
        }

        for (int i = 0; i < threads.length; i++) {
            threads[i].start();
        }
        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
        }
    }

    private static Thread getProducerThread(String name, String bootstrap, String topic, int speed, long batch, ObjectFaker faker, int instanceCount, int instanceIndex){
        return new Thread(name){
            @Override
            public void run() {
                try {
                    Properties props = new Properties();
                    props.put("bootstrap.servers", bootstrap);
                    props.put("acks", "1");
                    props.put("retries", 0);
                    props.put("linger.ms", 10);
                    props.put("compression.type", "snappy");
                    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

                    long start;
                    Map<String, Object> map;

                    try(KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
                        faker.init(instanceCount, instanceIndex);
                        int currentBatch = 0;
                        while (currentBatch < batch){
                            currentBatch ++;
                            start = System.currentTimeMillis();

                            for (int i = 0; i < speed; i++) {
                                map = faker.geneValue();
                                String ele = JSON.toJSONString(map);
                                //System.out.println(ele);
                                producer.send(new ProducerRecord<>(topic, ele));
                            }

                            long t = System.currentTimeMillis() - start;
                            if(t < 1000){
                                Thread.sleep(1000 - t);
                            }

                            System.out.println(name +  "-" + new java.sql.Timestamp(System.currentTimeMillis()) + ":" + speed);
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }


            }
        };
    }
}
