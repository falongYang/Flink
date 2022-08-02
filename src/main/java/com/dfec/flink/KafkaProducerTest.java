package com.dfec.flink;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: yfl
 * \* Date: 2020/7/3
 * \* To change this template use File | Settings | File Templates.
 * \* Description:
 * \
 */

import org.apache.kafka.clients.producer.*;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Properties;
import java.util.Random;


public class KafkaProducerTest {

    public static void main(String[] args) throws Exception {
        System.out.println("当前时间戳为：" + System.currentTimeMillis());

        String producerTopic1 = "logan_producer";
        String producerTopic2 = "logan_csv5";
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        int totalMessageCount = 1000;
        for (int i = 0; i < totalMessageCount; i++) {
            int id = new Random().nextInt(10);
            long time = System.currentTimeMillis();
            int age = new Random().nextInt(80);
            String key = "{\"key_name\":" + "\"" + id + "\"}";
//            String values = "{\"id\":" + "\"" + id + "\"," +
//                    "\"name\":" + "\"" + "姓名-" + id + "\"," +
//                    "\"age\":" + "\"" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) + "\"}";
////
//          String values = "{\"user_id\":" + "" + id + "," +
//                    "\"user_region\":" + "\"" + "姓名-" + id + "\"," +
//                    "\"viewtime\":" + "\"" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) + "\"}";

//            String values = "{\"application_name\":\"ecss-web\",\"url\":\"/ecs/webapp/web/paperJob/paperJob11\",\"elapsed\":" + i+322 + ",\"err_code\":\"0\",\"agent_id\":\"uqos-1\",\"send_time\":"+System.currentTimeMillis()+",\"parent_applicationId\":\"1111\",\"ip\":\"10.26.60.24\"}";

            // 测试正常的csv
//            String values = "\"" + id + "\"," +
//                    "\"" + "姓名-" + id + "\"," +
//                    "\"" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) + "\"," +
//                    "\"" + "12\";\"23\"";
            // 测试数组
            String values = + id + "," +
                    "姓名-" + id + "," +
                    LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) + "," +
                    "" + "12;23";


            System.out.println("开始生产消息：" + values);
            producer.send(new ProducerRecord<String, String>(producerTopic2, key, values), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception != null) {
                        System.out.println("Failed to send message with exception " + exception);
                    }
                }
            });
            Thread.sleep(1000);
        }
        producer.close();
    }

    private static long currentMemSize() {
        return MemoryUsageExtrator.currentFreeMemorySizeInBytes();
    }
}
