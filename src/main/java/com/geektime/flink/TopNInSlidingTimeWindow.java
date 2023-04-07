package com.geektime.flink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.File;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * https://xyueji.gitee.io/Flink%E5%AD%A6%E4%B9%A0--%E5%A6%82%E4%BD%95%E8%AE%A1%E7%AE%97%E5%AE%9E%E6%97%B6%E7%83%AD%E9%97%A8%E5%95%86%E5%93%81.html
 *
 */
public class TopNInSlidingTimeWindow {

    public static void main(String[] args) throws Exception {
//        EnvironmentSettings environmentSettings= EnvironmentSettings.newInstance()
//                .inBatchMode()
//                .build();
//        TableEnvironment tEnv = TableEnvironment.create(environmentSettings);
        PojoTypeInfo pojoTypeInfo = (PojoTypeInfo) TypeExtractor.createTypeInfo(UserBehavior.class);
// 字段顺序
        String[] fieldNames = new String[]{"userId", "itemId", "categoryId", "behavior", "timestamp"};
// csv InputFormat
        String filepath="datas/UserBehavior2.csv";
        // * 阿里天池的官方数据集，感觉有问题，待清洗
        // * https://tianchi.aliyun.com/dataset/649?t=1680785499836
//        filepath="F://UserBehavior.csv";
        Path localFile = Path.fromLocalFile(new File(filepath));
        PojoCsvInputFormat csvInputFormat = new PojoCsvInputFormat<>(localFile, pojoTypeInfo, fieldNames);
// 创建Flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
// 模拟真实数据流，这里并行度设为1
        env.setParallelism(1);
//        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
// 添加source
        DataStreamSource dataStreamSource = env.createInput(csvInputFormat, pojoTypeInfo);
        // 这里要设置EventTime, 默认ProcessingTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream timeData = dataStreamSource.assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps().withTimestampAssigner((event, timestamp) -> ((UserBehavior)event).getTimestamp()*1000L));
        DataStream pvData = timeData.filter((FilterFunction<UserBehavior>) userBehavior -> userBehavior.getBehavior().equals("pv"));

//        pvData.print();

        DataStream windowData = pvData
                .keyBy((KeySelector) o -> ((UserBehavior)o).getItemId()) //以itemId分组
                .window(SlidingEventTimeWindows.of(Time.minutes(60),Time.minutes(5)))
                .aggregate(new CountAgg(), new WindowResultFunction());

        DataStream topItems = windowData.keyBy("windowEnd").process(new TopNHotItems(5));

        topItems.print();
        env.execute("Hot Items Job");
    }

    /**
     * 求某个窗口中前 N 名的热门点击商品，key 为窗口时间戳，输出为 TopN 的结果字符串
     */
    public static class TopNHotItems extends KeyedProcessFunction<Long, ItemViewCount, String> {
        private final int topSize;
        public TopNHotItems(int topSize) {
            this.topSize = topSize;
        }
        // 用于存储商品与点击数的状态，待收齐同一个窗口的数据后，再触发 TopN 计算
        private ListState<ItemViewCount> itemState;
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // 状态的注册
            ListStateDescriptor<ItemViewCount> itemsStateDesc = new ListStateDescriptor<>(
                    "itemState-state",
                    ItemViewCount.class);
            itemState = getRuntimeContext().getListState(itemsStateDesc);
        }
        @Override
        public void processElement(
                ItemViewCount input,
                Context context,
                Collector<String> collector) throws Exception {
            // 每条数据都保存到状态中
            itemState.add(input);
            // 注册 windowEnd+1 的 EventTime Timer, 当触发时，说明收齐了属于windowEnd窗口的所有商品数据
            context.timerService().registerEventTimeTimer(input.windowEnd + 1);//类似watermark
        }
        @Override
        public void onTimer(
                long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 获取收到的所有商品点击量
            List<ItemViewCount> allItems = new ArrayList<>();
            for (ItemViewCount item : itemState.get()) {
                allItems.add(item);
            }
            // 提前清除状态中的数据，释放空间
            itemState.clear();
            // 按照点击量从大到小排序
            allItems.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    return (int) (o2.viewCount - o1.viewCount);
                }
            });

            // 将排名信息格式化成 String, 便于打印
            StringBuilder result = new StringBuilder();
            result.append("====================================\n");
            result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n");
            for (int i = 0; i<topSize && i<allItems.size(); i++) {
                ItemViewCount currentItem = allItems.get(i);
                // No1:  商品ID=12224  浏览量=2413
                result.append("No").append(i+1).append(":")
                        .append("  商品ID=").append(currentItem.itemId)
                        .append("  浏览量=").append(currentItem.viewCount)
                        .append("\n");
            }
            result.append("====================================\n\n");
            // 控制输出频率，模拟实时滚动结果
            Thread.sleep(1000);
            out.collect(result.toString());
        }
    }

    public static class WindowResultFunction extends ProcessWindowFunction<Long,ItemViewCount,Long,TimeWindow>{
        @Override
        public void process(Long aLong, ProcessWindowFunction<Long, ItemViewCount, Long, TimeWindow>.Context context, Iterable<Long> iterable, Collector<ItemViewCount> collector) throws Exception {
            collector.collect(ItemViewCount.of(aLong,context.window().getEnd(),iterable.iterator().next()));
        }
    }

    /**
     * 商品点击量(窗口操作的输出类型)
     */
    public static class ItemViewCount {
        public long itemId;     // 商品ID
        public long windowEnd;  // 窗口结束时间戳
        public long viewCount;  // 商品的点击量

        public static ItemViewCount of(long itemId, long windowEnd, long viewCount) {
            ItemViewCount result = new ItemViewCount();
            result.itemId = itemId;
            result.windowEnd = windowEnd;
            result.viewCount = viewCount;
            return result;
        }
    }

    public static class CountAgg implements AggregateFunction<UserBehavior, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior userBehavior, Long acc) {
            return acc + 1;
        }

        @Override
        public Long getResult(Long acc) {
            return acc;
        }

        @Override
        public Long merge(Long acc1, Long acc2) {
            return acc1 + acc2;
        }
    }
}
