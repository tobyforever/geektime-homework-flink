package com.geektime.kafka;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 从kafka的topic test5中读出之前sink进来的消息数据，然后进行实时的单词次数统计
 */
public class FlinKafkaSourceByJava {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        //默认checkpoint功能是disabled的，想要使用的时候需要先启用
// 每隔5000 ms进行启动一个检查点【设置checkpoint的周期】
        executionEnvironment.enableCheckpointing(5000);

        executionEnvironment.enableCheckpointing(5000);
// 高级选项：
// 设置模式为exactly-once （这是默认值）
        executionEnvironment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
// 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
        executionEnvironment.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
// 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
        executionEnvironment.getCheckpointConfig().setCheckpointTimeout(60000);
// 同一时间只允许进行一个检查点
        executionEnvironment.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        executionEnvironment.getCheckpointConfig().setCheckpointStorage(new Path("hdfs://bigdata01:8020/flink_kafka_check"));
// 表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint【详细解释见备注】

/**
 * ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION:表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
 * ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION: 表示一旦Flink处理程序被cancel后，会删除Checkpoint数据，只有job执行失败的时候才会保存checkpoint
 */
        executionEnvironment.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //   executionEnvironment.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //允许使用checkpoint非对齐检查点
        executionEnvironment.getCheckpointConfig().enableUnalignedCheckpoints();

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("bigdata01:9092,bigdata02:9092,bigdata03:9093")
                .setTopics("test5")
                .setGroupId("consumer_test_group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema()).build();


        DataStreamSource<String> dataStreamSource = executionEnvironment.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");
        SingleOutputStreamOperator<String> flatMap = dataStreamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] eachWord = value.split(" ");
                for (String word : eachWord) {
                    out.collect(word);
                }
            }
        });

        //使用 map 函数将每个单词映射为一个 Tuple2 对象，其中第一个元素是单词本身，第二个元素是单词的计数（初始化为 1）：
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = flatMap.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                Tuple2<String, Integer> tuple2 = new Tuple2<>();
                tuple2.setFields(value, 1);
                return tuple2;
            }
        }).keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
//            对每个 Tuple2 对象按单词（第一个元素）进行分组，并对计数（第二个元素）求和：
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {

                return value.f0;
            }
        }).sum(1);

        //将处理结果输出到控制台：
        result.print();
        executionEnvironment.execute();
    }

}