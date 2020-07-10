package com.dfec.kudu.kudusourcce;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
import java.util.Map;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: yfl
 * \* Date: 2020/7/10
 * \* To change this template use File | Settings | File Templates.
 * \* Description:
 * \
 */
public class KuduSource2Test {

    public static void main(String[] args) throws Exception {
        String driver = "com.cloudera.impala.jdbc41.Driver";
        String url = "jdbc:impala://192.168.1.170:25004/kudu_data;auth=noSasl";
        String querySql = "SELECT * from surf_chn_mul_hor2 where station_id_c in (SELECT station_id_c from surf_chn_sta_info where is_gg = 1) " +
                "and time BETWEEN '2013-01-01 10:00:00' and '2013-01-20 10:00:00' limit 5000";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        long s = System.currentTimeMillis();
        DataStreamSource<List<MapData>> dataStreamSource = env.addSource(new KuduSource2(driver, url, querySql));

        dataStreamSource.print();

        env.execute();
        long e = System.currentTimeMillis();

        System.out.println("耗时：" + (e -s )/1000 + "s");

    }
}