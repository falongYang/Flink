package com.dfec.kudu.kudusink2;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: yfl
 * \* Date: 2020/7/9
 * \* To change this template use File | Settings | File Templates.
 * \* Description:
 * \
 */
import org.apache.kudu.Schema;

import java.io.Serializable;

public interface KuduSerialization<T> extends Serializable {
    KuduRow serialize(T value);

    KuduSerialization<T> withSchema(Schema schema);
}