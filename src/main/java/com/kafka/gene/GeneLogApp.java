package com.kafka.gene;

import com.alibaba.fastjson2.JSON;
import com.utils.ArgsTool;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.List;
import java.util.Properties;

public class GeneLogApp {

    // --bootstrap 192.168.216.86:9092 --topic first --batch 1200 --gene "[{\"type\":\"int_random\", \"fields\":{\"name\":\"id\"}}, {\"type\":\"str_fix\", \"fields\":{\"name\":\"name\",\"len\":5}}]"
    public static void main(String[] args) throws Exception {
        ArgsTool argsTool = ArgsTool.fromArgs(args);
        System.out.println(argsTool.getKwargs());
        final String bootstrap = argsTool.getRequired("bootstrap");
        final String topic = argsTool.getRequired("topic");

        final int speed = argsTool.getInt("speed", 1);
        final long batch = argsTool.getLong("batch", Long.MAX_VALUE);
        final int parallelism = argsTool.getInt("parallelism", 1);

        Thread[] threads = new Thread[parallelism];
        for (int i = 0; i < threads.length; i++) {
            List<AbstractFieldGene<?>> fieldGenes = FieldGeneUtils.createFieldGenesFromJson(argsTool.getRequired("gene"));
            threads[i] = getProducerThread("Thread:" +(i + 1) + "/" + parallelism,
                    bootstrap, topic, speed, batch, fieldGenes);
        }

        for (int i = 0; i < threads.length; i++) {
            threads[i].start();
        }
        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
        }
    }

    public static Thread getProducerThread(String name, String bootstrap, String topic, int speed, long batch, List<AbstractFieldGene<?>> fieldGenes){
        return new Thread(name){
            @Override
            public void run() {
                try {
                    for (int j = 0; j < fieldGenes.size(); j++) {
                        fieldGenes.get(j).open();
                    }

                    Properties props = new Properties();
                    props.put("bootstrap.servers", bootstrap);
                    props.put("acks", "1");
                    props.put("retries", 0);
                    props.put("linger.ms", 10);
                    props.put("compression.type", "snappy");
                    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

                    long start;
                    AbstractFieldGene fieldGene;
                    Object value;
                    HashMap<String, Object> map = new HashMap<>(fieldGenes.size() * 2);

                    try(KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
                        int currentBatch = 0;
                        while (currentBatch < batch){
                            currentBatch ++;
                            start = System.currentTimeMillis();

                            for (int i = 0; i < speed; i++) {
                                map.clear();
                                for (int j = 0; j < fieldGenes.size(); j++) {
                                    fieldGene = fieldGenes.get(j);
                                    value = fieldGene.geneValue();
                                    if(value != null){
                                        map.put(fieldGene.fieldName(), value);
                                    }
                                }

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
