package com.dfec.kudu.kudusink1;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: yfl
 * \* Date: 2020/7/7
 * \* To change this template use File | Settings | File Templates.
 * \* Description:继承了 RichSinkFunction，重写了 open、close 和 invoke 方法，
 * 在 open 中进行 kudu 相关配置的初始化，
 * 在 invoke 中进行数据写入的相关操作，
 * 在 close 中关掉所有的开关。
 * \
 */
public class SinkKudu extends RichSinkFunction<Map<String,Object>> {
    private final static Logger logger = Logger.getLogger(SinkKudu.class);
    private KuduClient client;
    private KuduTable table;
    private Schema schema;
    private KuduSession kuduSession;
    private ByteArrayOutputStream out;
    private ObjectOutputStream os;
    private String kuduMaster;
    private String tableName;

    public SinkKudu(String kuduMaster,String tableName){
        this.kuduMaster = kuduMaster;
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        out = new ByteArrayOutputStream();
        os = new ObjectOutputStream(out);
        client = new KuduClient.KuduClientBuilder(kuduMaster).build();
        System.out.println("client:" + client);
        table = client.openTable(tableName);
        schema = table.getSchema();
        kuduSession = client.newSession();
        kuduSession.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
    }

    @Override
    public void invoke(Map<String, Object> map, Context context) throws Exception {

        Set<String> keySet = map.keySet();
                for (String s : keySet) {
                    Object o = map.get(s);
                    System.out.println(s + "=" + o);
                }
        long l = context.currentWatermark();
        System.out.println("L:" + l);
        if (map == null){
            return;
        }
        System.out.println("===============");
        try {
            int columnCount = schema.getColumnCount();
            System.out.println("columnCount:" + columnCount);
            Insert insert = table.newInsert();
            PartialRow row = insert.getRow();
            for (int i = 0; i < columnCount; i++) {
                Object value = map.get(schema.getColumnByIndex(i).getName());
                System.out.println("schema.getColumnByIndex(i).getName()" + schema.getColumnByIndex(i).getName());
                insertData(row,schema.getColumnByIndex(i).getType(),schema.getColumnByIndex(i).getName(),value);
            }
            OperationResponse response = kuduSession.apply(insert);
            if (response != null){
                logger.error(response.getRowError().toString());
            }
        }catch (Exception e){
            logger.error(e);
        }
    }


    // 插入数据
    private void insertData(PartialRow row, Type type, String name, Object value) {
        System.out.println("value:===" + value);
        try {
            switch (type){
                case STRING:
                    row.addString(name,value.toString());
                    return;
                case INT32:
                    row.addInt(name,Integer.valueOf(value.toString()));
                    return;
                case INT64:
                    row.addLong(name,Long.valueOf(value.toString()));
                    return;
                case DOUBLE:
                    row.addDouble(name,Double.valueOf(value.toString()));
                    return;
                case BOOL:
                    row.addBoolean(name,(Boolean)value);
                    return;
                case INT8:
                    row.addByte(name, (byte) value);
                    return;
                case INT16:
                    row.addShort(name, (short) value);
                    return;
                case BINARY:
                    os.writeObject(value);
                    row.addBinary(name, out.toByteArray());
                    return;
                case FLOAT:
                    row.addFloat(name, Float.valueOf(String.valueOf(value)));
                    return;
                default:
                    throw new UnsupportedOperationException("Unknow type " + type);
            }
        } catch (Exception e) {
            logger.error("数据插入异常",e);
        }
    }

    @Override
    public void close() throws Exception {
        try {
            kuduSession.close();
            client.close();
            os.close();
            out.close();
        }catch (Exception e){
            logger.error(e);
        }
    }

}