package com.dfec.kudu.kudusourcce;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
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
public class KuduSource2 extends RichSourceFunction<List<MapData>> {

    private String driver;
    private String url;
    private String querySql;

    Connection conn;
    Statement st;

    public KuduSource2() {
    }

    public KuduSource2(String driver, String url, String querySql) {
        this.driver = driver;
        this.url = url;
        this.querySql = querySql;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        DriverManager.registerDriver(new com.cloudera.impala.jdbc41.Driver());
        conn = DriverManager.getConnection(url);
        st = conn.createStatement();
        System.out.println("Impala的JDBC连接为：" + conn);
    }

    @Override
    public void close() throws Exception {
        if (st != null){
            st.close();
        }
        if (conn != null){
            conn.close();
        }
    }

    @Override
    public void run(SourceContext<List<MapData>> ctx) throws Exception {
        ArrayList<MapData> list = new ArrayList<>();
        ResultSet rs = st.executeQuery(querySql);
        setResult(list,rs);
        ctx.collect(list);
    }

    private void setResult(List<MapData> list, ResultSet rs) throws SQLException {
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnCount = rsmd.getColumnCount();
        MapData map;
        while (rs.next()){
            map = new MapData();
            //HashMap<String, Object> map = new HashMap<>();
            for (int i = 1; i <= columnCount ; i++) {
                String columnName = rsmd.getColumnLabel(i);
                Object value = rs.getObject(columnName);
                map.put(columnName,value);
                if (value instanceof Double) {
                    Double d = (Double) value;
                    if (d.isNaN())
                        map.put(columnName, null);
                } else if (value instanceof Float) {
                    Float d = (Float) value;
                    if (d.isNaN())
                        map.put(columnName, null);
                }
            }
            list.add(map);
        }
    }

    @Override
    public void cancel() {

    }
}