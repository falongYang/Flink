package com.dfec.kudu.kudusink1;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.Map;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: yfl
 * \* Date: 2020/7/7
 * \* To change this template use File | Settings | File Templates.
 * \* Description:
 * \
 */
public class SinkTest {
    public static void main(String[] args) throws Exception {
        // 初始化flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 生成数据源
        DataStreamSource<UserInfo> dataStreamSource = env.fromElements(
                new UserInfo("001", "zhangsan", 21),
                new UserInfo("002", "lisi", 21),
                new UserInfo("003", "wangwu", 22),
                new UserInfo("004", "liupi", 20),
                new UserInfo("005", "liba", 23));

        // 转换数据 map
        SingleOutputStreamOperator<Map<String, Object>> mapSource = dataStreamSource.map(new MapFunction<UserInfo, Map<String, Object>>() {
            @Override
            public Map<String, Object> map(UserInfo value) throws Exception {
                HashMap<String, Object> map = new HashMap<>();
                map.put("id", value.getId());
                map.put("name", value.getName());
                map.put("age", value.getAge());
//                Set<String> keySet = map.keySet();
//                for (String s : keySet) {
//                    Object o = map.get(s);
//                    System.out.println(s + "=" + value);
//                }
                return map;
            }
        });

        // Sink到Kudu
        String kuduMaster = "192.168.1.181";
        String tableName = "impala::ods.test_flink_kudu_yfl";
        mapSource.addSink(new SinkKudu(kuduMaster,tableName));


        dataStreamSource.print();

        env.execute("sink-kudu-test");

    }
}