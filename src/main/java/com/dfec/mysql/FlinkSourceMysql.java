package com.dfec.mysql;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: yfl
 * \* Date: 2020/7/8
 * \* To change this template use File | Settings | File Templates.
 * \* Description:
 * \
 */
public class FlinkSourceMysql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        MysqlSourceUtils mysqlSourceUtils = new MysqlSourceUtils();
        DataStreamSource dataStreamSource = env.addSource(mysqlSourceUtils);

        // 测试转换
        // map
        SingleOutputStreamOperator map = dataStreamSource.map(new MapFunction<Student, Student>() {
            @Override
            public Student map(Student value) throws Exception {
                Student s1 = new Student();
                s1.id = value.id;
                s1.name = value.name;
                s1.password = value.password;
                s1.age = value.age + 5;
                return s1;
            }
        });
        // flatmap
        SingleOutputStreamOperator flatMap = dataStreamSource.flatMap(new FlatMapFunction<Student, Student>() {
            @Override
            public void flatMap(Student value, Collector<Student> out) throws Exception {
                if (value.id % 2 == 0) {
                    out.collect(value);
                }
            }
        });
        // filter : 根据条件判断得出结果
        SingleOutputStreamOperator filter = dataStreamSource.filter(new FilterFunction<Student>() {
            @Override
            public boolean filter(Student value) throws Exception {
                if (value.id > 3) {
                    return true;
                }
                return false;
            }
        });
        // KeyBy 在逻辑上市基于Key对流进行分区，
        // 在内部，它使用hash函数对流进行分区，他返回keyedDataStream数据流
        KeyedStream keyBy = dataStreamSource.keyBy(new KeySelector<Student, Integer>() {
            @Override
            public Integer getKey(Student value) throws Exception {
                return value.age;
            }
        });
        // Reduce reduce返回单个的结果值，并且reduce操作每处理一个元素总是创建一个新值
        // 常用的方法有 average, sum, min, max, count，使用 reduce 方法都可实现
        SingleOutputStreamOperator reduce = keyBy.reduce(new ReduceFunction<Student>() {
            // 先将数据流进行keyby操作，因为reduce操作只能是keyedStream
            // 然后将student对象的age做了求平均值的操作
            @Override
            public Student reduce(Student value1, Student value2) throws Exception {
                Student s2 = new Student();
                s2.name = value1.name + value2.name;
                s2.id = (value1.id + value2.id) / 2;
                s2.password = value1.password + value2.password;
                s2.age = (value1.age + value2.age) / 2;
                return s2;
            }
        });
        // fold 通过将一个文件夹流与当前记录组合来推出keyedStream


        //dataStreamSource.print();
        //map.print();
        //flatMap.print();
        //filter.print();
        //keyBy.print();
        //reduce.print();
        env.execute("flink source mysql");

    }
}