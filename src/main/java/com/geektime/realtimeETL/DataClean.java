package com.geektime.realtimeETL;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Properties;

/**
 * 实时 ETL
 */
public class DataClean {
    public static void main(String[] args) throws Exception{
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);// 假设 Kafka 的主题是 3 个分区
        // 设置 checkpoint
        env.enableCheckpointing(60000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //flink 停止的时候要不要清空 checkpoint 的数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        env.setStateBackend(new RocksDBStateBackend("hdfs://bigdata02:9000/FlinkETL/checkpoint"));

        env.setStateBackend(new RocksDBStateBackend("hdfs://bigdata01:8020/FlinkETL/checkpoint"));
        //Kafka 数据源
        String topic="data";
        Properties properties = new Properties();
        properties.put("bootstrap.servers","bigdata03:9092");
        properties.put("group.id","dataclean_consumer");
        properties.put("enable.auto.commit","false");
        properties.put("auto.offset.reset","earliest");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                topic,
                new SimpleStringSchema(),
                properties
        );
        DataStreamSource<String> allData = env.addSource(consumer);

        // redis
        DataStream<HashMap<String, String>> mapData = env.addSource(new RedisSource()).broadcast();

        SingleOutputStreamOperator<String> etlDataStream = allData.connect(mapData).flatMap(new CoFlatMapFunction<String, HashMap<String, String>, String>() {
            // 其实不给也行。
            HashMap<String, String> allMap = new HashMap<String, String>();

            // 在这儿一开始，我们还是需要给 allmap 一些初始的数据。


            //alldata kafka
            @Override
            public void flatMap1(String line, Collector<String> collector) throws Exception {
//{"dt":"2019-11-19 20:33:39","countryCode":"TW","data":[{"type":"s1","score":0.8,"level":"D"},{"type":"s2","score":0.1,"level":"B"}]}
                JSONObject jsonObject = JSONObject.parseObject(line);
                String dt = jsonObject.getString("dt");
                String countryCode = jsonObject.getString("countryCode");
                // 根据省份获取大区
                String area = allMap.get(countryCode);
                JSONArray data = jsonObject.getJSONArray("data");
                for (int i = 0; i < data.size(); i++) {
                    //0 {"type":"s1","score":0.8,"level":"D"}
                    //1 {"type":"s2","score":0.1,"level":"B"}
                    JSONObject dataJSONObject = data.getJSONObject(i);
                    // 添加日期
                    dataJSONObject.put("dt", dt);
                    // 添加大区
                    dataJSONObject.put("area", area);
                    collector.collect(dataJSONObject.toString());
                }

            }

            //mapdata redis
            @Override
            public void flatMap2(HashMap<String, String> map, Collector<String> collector) throws Exception {
                allMap = map;
            }
        });

        // etlDataStream.print().setParallelism(1);
        String etltopic="etldata";
        Properties sinkProperties = new Properties();
        sinkProperties.put("bootstrap.servers","bigdata03:9092");
        FlinkKafkaProducer<String> kafkaSink = new FlinkKafkaProducer(etltopic,
                new SimpleStringSchema(),
                sinkProperties);

        etlDataStream.addSink(kafkaSink);

       env.execute("data clean");

        //到bigdata02下启动redis。注意要修改bind为0.0.0.0还要CONFIG SET protected-mode no
        //运行kafkaProducerDatas启动生产数据
        //运行DataClean启动实时ETL

        // 查看输出的topic里的结果 ./kafka-console-consumer.sh --bootstrap-server bigdata03:9092 --topic etldata
        //{"dt":"2023-04-11 14:52:08","area":"AREA_CT","score":0.2,"level":"A+","type":"s3"}
        //{"dt":"2023-04-11 14:52:08","area":"AREA_CT","score":0.8,"level":"A+","type":"s1"}
        //{"dt":"2023-04-11 14:52:10","area":"AREA_IN","score":0.3,"level":"C","type":"s5"}
    }
}
