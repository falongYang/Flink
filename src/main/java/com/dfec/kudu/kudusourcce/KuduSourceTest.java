package com.dfec.kudu.kudusourcce;

import com.dfec.kudu.kudusink2.TestPojo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: yfl
 * \* Date: 2020/7/10
 * \* To change this template use File | Settings | File Templates.
 * \* Description:
 * \
 */
public class KuduSourceTest {
    public static void main(String[] args) throws Exception {
        String kuduMaster = "192.168.1.170";
        String tableName = "impala::kudu_data.test_flink_kudu_yfl2";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource dataStreamSource = env.addSource(new KuduSource(kuduMaster, tableName));

        dataStreamSource.print();

        env.execute();

    }
}