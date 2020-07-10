package com.dfec.kudu;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: yfl
 * \* Date: 2020/7/9
 * \* To change this template use File | Settings | File Templates.
 * \* Description:
 * \
 */
import org.apache.flink.api.common.io.GenericInputFormat;
import org.apache.flink.core.io.GenericInputSplit;


import java.io.IOException;


public class KuduInputFormat extends GenericInputFormat<String> {

    @Override
    public void open(GenericInputSplit split) throws IOException {
        super.open(split);
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return false;
    }

    @Override
    public String nextRecord(String reuse) throws IOException {
        return null;
    }

    @Override
    public GenericInputSplit[] createInputSplits(int numSplits) throws IOException {
        return super.createInputSplits(numSplits);
    }

    @Override
    public void close() throws IOException {
        super.close();
    }
}