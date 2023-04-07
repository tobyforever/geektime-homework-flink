package com.geektime.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 运行这个flink任务，用nc在bigdata01上启动一个socket在9999端口，输入socket的消息被flink存入kafka
 *
 * 在 bigdata01 机器上，您可以使用 nc（Netcat）工具启动一个简单的 socket 服务器，监听 9999 端口。如果 nc 未安装在您的系统中，请首先安装它。在大多数 Linux 发行版中，您可以使用包管理器（如 apt、yum 或 pacman）来安装 nc。
 *
 * 在安装 nc 之后，您可以通过运行以下命令在 bigdata01 机器上启动 socket 服务器：
 *
 * nc -lk 9999
 * 此命令将启动一个 socket 服务器，监听 9999 端口。服务器将保持运行状态，直到您手动停止它。在此模式下，您可以通过控制台直接输入消息，然后按回车键发送。当有客户端连接到此服务器时，它们将接收到您通过控制台输入的消息。要停止服务器，只需按 Ctrl + C。
 *
 * 请注意，如果您希望在后台启动 socket 服务器并将其输出重定向到文件，可以使用以下命令：
 *
 * nc -lk 9999 > output.log 2>&1 &
 * 此命令将在后台启动 socket 服务器，并将所有输出（包括发送的消息）重定向到名为 output.log 的文件中。要停止在后台运行的服务器，您需要使用 kill 命令终止其进程。首先，使用 ps 和 grep 命令找到服务器进程的 ID：
 *
 * ps aux | grep "nc -lk 9999"
 * 然后使用 kill 命令终止该进程：
 *
 * kill [process_id]
 * 请将 [process_id] 替换为实际的进程 ID。
 */
public class FlinKafkaSinkByJava {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();


     //   executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


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

        EmbeddedRocksDBStateBackend embeddedRocksDBStateBackend = new EmbeddedRocksDBStateBackend(true);

        //设置状态保存方式
        executionEnvironment.setStateBackend(embeddedRocksDBStateBackend);


// 同一时间只允许进行一个检查点
        executionEnvironment.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        executionEnvironment.getCheckpointConfig().setCheckpointStorage(new Path("hdfs://bigdata01:8020/flink_kafka_sink_check"));
// 表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint【详细解释见备注】

/**
 * ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION:表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
 * ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION: 表示一旦Flink处理程序被cancel后，会删除Checkpoint数据，只有job执行失败的时候才会保存checkpoint
 */
        executionEnvironment.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //   executionEnvironment.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //允许使用checkpoint非对齐检查点
        executionEnvironment.getCheckpointConfig().enableUnalignedCheckpoints();


        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("bigdata01:9092,bigdata02:9092,bigdata03:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("test5")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        DataStreamSource<String> dataStreamSource = executionEnvironment.socketTextStream("bigdata01", 9999);
        dataStreamSource.sinkTo(kafkaSink);

        executionEnvironment.execute();
    }
}