package com.dfec.kudu.kudusourcce;

import com.dfec.kudu.kudusink2.TestPojo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.*;

import java.util.List;


/**
 * \* Created with IntelliJ IDEA.
 * \* User: yfl
 * \* Date: 2020/7/9
 * \* To change this template use File | Settings | File Templates.
 * \* Description:在 open 中进行 kudu 相关配置的初始化，
 *  * 在 invoke 中进行数据写入的相关操作，
 *  * 在 close 中关掉所有的开关。
 *
 *  扫描全表查询，返回所有结果
 * \
 */
public class KuduSource extends RichSourceFunction<TestPojo> {
    private String kuduMaster;
    private String tableName;

    KuduClient client;

    KuduScanner.KuduScannerBuilder builder;

    public KuduSource(){}

    public  KuduSource(String kuduMaster,String tableName){
        this.kuduMaster = kuduMaster;
        this.tableName = tableName;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        client = new KuduClient.KuduClientBuilder(kuduMaster).build();
        KuduTable table = client.openTable(tableName);
        builder = client.newScannerBuilder(table);

    }

    @Override
    public void run(SourceContext ctx) throws Exception {
        TestPojo testPojo = null;
        KuduScanner scanner = builder.build();
        while (scanner.hasMoreRows()){
            RowResultIterator iterator = scanner.nextRows();
            while (iterator.hasNext()){
                RowResult result = iterator.next();

//            Schema schema = result.getSchema();
//            List<ColumnSchema> columns = schema.getColumns();

                testPojo = new TestPojo(
                        result.getInt("id"),
                        result.getString("name")
                );

                ctx.collect(testPojo);
            }

        }

    }

    @Override
    public void cancel() {

    }

    @Override
    public void close() throws Exception {
        if (client != null){
            client.close();
        }

    }
}