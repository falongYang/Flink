package com.dfec.kudu.kudusink2;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: yfl
 * \* Date: 2020/7/9
 * \* To change this template use File | Settings | File Templates.
 * \* Description:
 * \
 */
public class KuduSinkTest2 {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "192.168.1.171:9092");
        props.setProperty("group.id", "flink_1");
        StreamExecutionEnvironment ENV =
                StreamExecutionEnvironment.getExecutionEnvironment();
        ENV.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<String> kafkaSource = ENV.addSource(
                new FlinkKafkaConsumer011<>(
                        "estest",
                        new SimpleStringSchema(),
                        props
                )
        );

        kafkaSource.print();

        KuduTableInfo kuduTableInfo = KuduTableInfo.Builder.open("impala::kudu_data.test_flink_kudu_yfl2").build();
        kafkaSource.map((MapFunction<String, TestPojo>) value -> JSON.parseObject(value, TestPojo.class))
                .addSink(new KuduSink<TestPojo>("192.168.1.170", kuduTableInfo, TestPojo.class).withStrongConsistency());
        ENV.execute();
    }
    }
