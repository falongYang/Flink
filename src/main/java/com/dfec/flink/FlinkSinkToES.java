package com.dfec.flink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: yfl
 * \* Date: 2020/7/3
 * \* To change this template use File | Settings | File Templates.
 * \* Description:
 * \
 */
public class FlinkSinkToES {
    private static final Logger log = LoggerFactory.getLogger(FlinkSinkToES.class);

    private static final String READ_TOPIC = "estest";

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.1.171:9092");
        props.put("zookeeper.connect", "192.168.1.171:2181");
        props.put("group.id", "es-flink");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");

//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> student = env.addSource(new FlinkKafkaConsumer010<String>(
                //这个 kafka topic 需要和上面的工具类的 topic 一致
                READ_TOPIC,
                new SimpleStringSchema(),
                props)).setParallelism(1);
//                .map(string -> JSON.parseObject(string, Student.class)); //Fastjson 解析字符串成 student 对象
        Map<String, String> config = new HashMap<>();
        config.put("cluster.name", "elasticsearch");
// This instructs the sink to emit after every element, otherwise they would be buffered
        config.put("bulk.flush.max.actions", "1");
//        config.put("auth_user","elastic");
//        config.put("auth_password","changeme");

        List<InetSocketAddress> transportAddresses = new ArrayList<>();
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("192.168.1.170"), 9200));
//        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("10.2.3.1"), 9300));

        ElasticsearchSinkFunction<String> elasticsearchSinkFunction = new ElasticsearchSinkFunction<String>() {
            @Override
            public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
//                HashMap<String, String> hashMap = new HashMap<>();
//                hashMap.put("data",element);
                //hashMap.forEach((x,y) -> System.out.println(x=y));


                JSONObject str = JSON.parseObject(element);
                System.out.println("========" + str);

                IndexRequest source = Requests.indexRequest().index("es_flink").type("_doc").source(str);
                indexer.add(source);
            }
        };


        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("192.168.1.170",9200,"http"));



        // 有问题
        ElasticsearchSink.Builder<String> stringBuilder = new ElasticsearchSink.Builder<>(httpHosts, elasticsearchSinkFunction);
        stringBuilder.setBulkFlushMaxActions(10);
        ElasticsearchSink<String> build = stringBuilder.build();

        student.addSink(build);
        student.print();

        env.execute();
    }
}

