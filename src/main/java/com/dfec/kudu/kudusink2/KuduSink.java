package com.dfec.kudu.kudusink2;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: yfl
 * \* Date: 2020/7/9
 * \* To change this template use File | Settings | File Templates.
 * \* Description:
 * \
 */
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class KuduSink<OUT> extends RichSinkFunction<OUT> {

    private static final Logger LOG = LoggerFactory.getLogger(KuduSink.class);

    private static final long serialVersionUID = -893553371298868553L;

    private String kuduMasters;
    private KuduTableInfo tableInfo;
    private KuduConnector.Consistency consistency;
    private KuduConnector.WriteMode writeMode;

    private KuduSerialization<OUT> serializer;

    private transient KuduConnector connector;

    private Class aClass;

    public KuduSink(String kuduMasters, KuduTableInfo tableInfo, KuduSerialization<OUT> serializer) {
        Preconditions.checkNotNull(kuduMasters, "kuduMasters could not be null");
        this.kuduMasters = kuduMasters;

        Preconditions.checkNotNull(tableInfo, "tableInfo could not be null");
        this.tableInfo = tableInfo;
        this.consistency = KuduConnector.Consistency.STRONG;
        this.writeMode = KuduConnector.WriteMode.UPSERT;
        this.serializer = serializer.withSchema(tableInfo.getSchema());
    }


    public KuduSink(String kuduMasters, KuduTableInfo tableInfo, Class aClass) {
        Preconditions.checkNotNull(kuduMasters, "kuduMasters could not be null");
        this.kuduMasters = kuduMasters;

        Preconditions.checkNotNull(tableInfo, "tableInfo could not be null");
        this.tableInfo = tableInfo;
        this.consistency = KuduConnector.Consistency.STRONG;
        this.writeMode = KuduConnector.WriteMode.UPSERT;
        this.aClass = aClass;
    }

    public KuduSink<OUT> withEventualConsistency() {
        this.consistency = KuduConnector.Consistency.EVENTUAL;
        return this;
    }

    public KuduSink<OUT> withStrongConsistency() {
        this.consistency = KuduConnector.Consistency.STRONG;
        return this;
    }

    public KuduSink<OUT> withUpsertWriteMode() {
        this.writeMode = KuduConnector.WriteMode.UPSERT;
        return this;
    }

    public KuduSink<OUT> withInsertWriteMode() {
        this.writeMode = KuduConnector.WriteMode.INSERT;
        return this;
    }

    public KuduSink<OUT> withUpdateWriteMode() {
        this.writeMode = KuduConnector.WriteMode.UPDATE;
        return this;
    }

    @Override
    public void open(Configuration parameters) throws IOException {
        if (connector != null) {
            return;
        }
        connector = new KuduConnector(kuduMasters, tableInfo, consistency, writeMode);
//        serializer.withSchema(tableInfo.getSchema());
    }


    @Override
    public void invoke(OUT value, Context context) throws Exception {
//        KuduRow kuduRow = serializer.serialize(value);
//        boolean response = connector.writeRow(kuduRow);
        HbaseBeanFieldModel[] hbaseBeanFieldModels = KuduUtils.getBeanFieldModels(aClass);
        boolean response = false;
        if (null != hbaseBeanFieldModels) {
            response = connector.writeRow(hbaseBeanFieldModels, value);
        }

        if (!response) {
            throw new IOException("error with some transaction");
        }
    }

    @Override
    public void close() throws Exception {
        if (this.connector == null) {
            return;
        }
        try {
            this.connector.close();
        } catch (Exception e) {
            throw new IOException(e.getLocalizedMessage(), e);
        }
    }
}