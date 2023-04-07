package com.geektime.flink;
import com.google.common.collect.Lists;
import org.apache.commons.lang.time.DateUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
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
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.File;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

/**
 *
 */
public class TopUrl {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置时间时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStream<ApacheLogDto> dataStream = env.readTextFile("datas/apachelog.log")
                .flatMap(new MyFlatMapFunction())
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<ApacheLogDto>() {
                    @Override
                    public long extractAscendingTimestamp(ApacheLogDto apacheLogDto) {
                        return apacheLogDto.getTimestamp();
                    }
                });

        // 分组开窗计算
        SingleOutputStreamOperator<BehaviorUriOutWindow> aggStream = dataStream.keyBy(ApacheLogDto::getUrl) // 按url分组
//                .timeWindow(Time.seconds(60), Time.seconds(5)) // 开窗 长度60s 每5s滑动一次
                .window(SlidingEventTimeWindows.of(Time.seconds(60),Time.seconds(5)))
                .aggregate(new CountAgg(), new AggResultWinFunction());// 聚合计算


        // 按窗口分区 计算访问量高前N个URI
        aggStream.keyBy(BehaviorUriOutWindow::getWindowEnd).process(new MyUrlKeyProcessFunction(5))
                .print("hot url:"); // 输出

        env.execute();
    }


    /**
     * String 转 dto
     * in: 79.185.184.23 - - 17/05/2015:12:05:31 +0000 GET /reset.css
     */
    public static class MyFlatMapFunction implements FlatMapFunction<String, ApacheLogDto> {

        private SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");

        private String regex = "^((?!\\.(css|js|png|ico|html|txt|jar|jpg)$).)*$";

        @Override
        public void flatMap(String s, Collector<ApacheLogDto> collector) throws Exception {
            String[] split = s.split(" ");
            ApacheLogDto apacheLogDto = new ApacheLogDto();
            apacheLogDto.setIpAddr(split[0]);
            apacheLogDto.setTimestamp(simpleDateFormat.parse(split[3]).getTime());
            apacheLogDto.setMethod(split[5]);
            apacheLogDto.setUrl(split[6].split("\\?")[0]); // 切割问号
            // 过滤css js png ico html txt jpg后缀的url
            if (Pattern.matches(regex, apacheLogDto.getUrl())) {
                collector.collect(apacheLogDto);
            }
        }
    }


    /**
     * 聚合计算
     */
    public static class CountAgg implements AggregateFunction<ApacheLogDto, Integer, Integer> {

        // 初始值
        @Override
        public Integer createAccumulator() {
            return 0;
        }

        // 聚合逻辑
        @Override
        public Integer add(ApacheLogDto apacheLogDto, Integer acc) {
            return acc + 1;
        }

        // 结果值
        @Override
        public Integer getResult(Integer acc) {
            return acc;
        }

        // 合并操作
        @Override
        public Integer merge(Integer acc, Integer acc1) {
            return acc + acc1;
        }
    }

    /**
     * 窗口关闭 转换
     */
    public static class AggResultWinFunction implements WindowFunction<Integer, BehaviorUriOutWindow, String, TimeWindow> {

        @Override
        public void apply(String key, TimeWindow timeWindow, Iterable<Integer> iterable, Collector<BehaviorUriOutWindow> collector) throws Exception {
            // 获取当前窗口聚合计算结果
            Integer aggCount = iterable.iterator().next();
            BehaviorUriOutWindow outDto = new BehaviorUriOutWindow();
            outDto.url=key;
            outDto.viewCount=aggCount;
            outDto.windowEnd=timeWindow.getEnd();
            collector.collect(outDto);
        }
    }

    public static class BehaviorUriOutWindow {
        public String url;     // url
        public long windowEnd;  // 窗口结束时间戳
        public int viewCount;  // 商品的点击量

        public static TopUrl.BehaviorUriOutWindow of(String url, long windowEnd, int viewCount) {
            BehaviorUriOutWindow result = new BehaviorUriOutWindow();
            result.url = url;
            result.windowEnd = windowEnd;
            result.viewCount = viewCount;
            return result;
        }

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public long getWindowEnd() {
            return windowEnd;
        }

        public void setWindowEnd(long windowEnd) {
            this.windowEnd = windowEnd;
        }

        public int getViewCount() {
            return viewCount;
        }

        public void setViewCount(int viewCount) {
            this.viewCount = viewCount;
        }

        @Override
        public String toString() {
            SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM:dd HH:mm:ss");
            Date d=new Date();
            d.setTime(windowEnd);
            return sdf.format(d)+" url='" + url + '\'' +
                    ", viewCount=" + viewCount;
        }
    }

    /**
     * 计算访问量高前N个URI并输出
     */
    public static class MyUrlKeyProcessFunction extends KeyedProcessFunction<Long, BehaviorUriOutWindow, BehaviorUriOutWindow> {

        private Integer topNum = 1;

        private ListState<BehaviorUriOutWindow> listState;

        public MyUrlKeyProcessFunction(Integer topNum) {
            this.topNum = topNum;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            this.listState = getRuntimeContext().getListState(new ListStateDescriptor<BehaviorUriOutWindow>("host-url", BehaviorUriOutWindow.class));
        }

        @Override
        public void processElement(BehaviorUriOutWindow behaviorUriOutWindow, Context context, Collector<BehaviorUriOutWindow> collector) throws Exception {
            this.listState.add(behaviorUriOutWindow);
            // 注册定时器  注册时间一致 flink框架会自动覆盖掉
            context.timerService().registerEventTimeTimer(behaviorUriOutWindow.windowEnd + 1000L);
        }

        // 定时任务触发逻辑
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<BehaviorUriOutWindow> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            // 1. 按点击量降序排序
            List<BehaviorUriOutWindow> list = Lists.newArrayList(listState.get().iterator());
            list.sort(new Comparator<BehaviorUriOutWindow>() {
                @Override
                public int compare(BehaviorUriOutWindow o1, BehaviorUriOutWindow o2) {
                    return o2.viewCount - o1.viewCount;
                }
            });

//             2. 输出前topNum
            if (list.size() > topNum) {
                for (int i = 0; i < topNum; i++) {
                    out.collect(list.get(i));
                }
            } else {
                list.forEach(dto -> {
                    out.collect(dto);
                });
            }
        }
    }
}
