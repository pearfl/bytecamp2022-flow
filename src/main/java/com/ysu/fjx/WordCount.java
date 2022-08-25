package com.ysu.fjx;

import com.ysu.fjx.bean.ItemViewCount;
import com.ysu.fjx.bean.WordSource;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.net.URL;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WordCount {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
       env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);//基于事件时间

        // 2. 读取数据，创建DataStream
//        URL resource = WordSource.class.getResource("/word");
//        DataStream<String> inputStream = env.readTextFile(resource.getPath());

        //连接kafka集群
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092");
        // 配置消费者组（组名任意起名） 必须
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        DataStream<String> inputStream = env.addSource(new FlinkKafkaConsumer<String>("wxl", new SimpleStringSchema(), properties));


        // 3. 转换为POJO，分配时间戳和watermark
        DataStream<WordSource> dataStream = inputStream
                .map(line -> {
                    String[] fields = line.split(" ");
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
                    Long timestamp = simpleDateFormat.parse(fields[2]).getTime();
                    return new WordSource(fields[0].toLowerCase(), new Long(fields[1]), timestamp, fields[3]);
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<WordSource>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(WordSource element) {
                        return element.getTimestamp();
                    }
                });

        dataStream.print("data");

        StreamingFileSink<String> fileSink = StreamingFileSink
                .<String>forRowFormat(new Path("./output"),
                        new SimpleStringEncoder<>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build())
                .build();

        // 将Event转换成String写入文件
        dataStream.map(WordSource::toString).addSink(fileSink);

        // 4. 分组开窗聚合，得到每个窗口内各个商品的count值
        DataStream<ItemViewCount> windowAggStream = dataStream
                .filter(data -> "yes".equals(data.getIgnore()))    // 过滤yes行为
                .keyBy("word")    // 按商品ID分组
                .timeWindow(Time.minutes(5)) // 开一个时间为5分钟的滚动窗口
                .allowedLateness(Time.minutes(1))
                .aggregate(new ItemCountAgg(),new WindowItemCountResult());

        // 5. 收集同一窗口的所有单词的个数，排序输出top n
        DataStream<String> resultStream = windowAggStream
                .keyBy("windowEnd")  // 按照窗口分组
                .process(new TopNHotItems(5));   // 用自定义处理函数排序取前5

        resultStream.print();

        env.execute("word count analysis");
    }

    // 实现自定义增量聚合函数
    public static class ItemCountAgg implements AggregateFunction<WordSource, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(WordSource value, Long accumulator) {
            return accumulator + value.getCount();
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    // 自定义全窗口函数
    public static class WindowItemCountResult implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {
        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out) throws Exception {
            String word = tuple.getField(0);
            Long windowEnd = window.getEnd();
            Long count = input.iterator().next();
            out.collect(new ItemViewCount(word, windowEnd, count));
        }
    }

    // 实现自定义KeyedProcessFunction
    public static class TopNHotItems extends KeyedProcessFunction<Tuple, ItemViewCount, String> {
        // 定义属性，top n的大小
        private Integer topSize;

        public TopNHotItems(Integer topSize) {
            this.topSize = topSize;
        }

        // 定义列表状态，保存当前窗口内所有输出的ItemViewCount
        ListState<ItemViewCount> itemViewCountListState;

        @Override
        public void open(Configuration parameters) throws Exception {
            itemViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCount>("item-view-count-list", ItemViewCount.class));
        }

        @Override
        public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
            // 每来一条数据，存入List中，并注册定时器
            itemViewCountListState.add(value);
            ctx.timerService().registerEventTimeTimer( value.getWindowEnd() + 1 );
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发，当前已收集到所有数据，排序输出
            ArrayList<ItemViewCount> itemViewCounts = Lists.newArrayList(itemViewCountListState.get().iterator());

            itemViewCounts.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    return o2.getCount().intValue() - o1.getCount().intValue();
                }
            });

            // 将排名信息格式化成String，方便打印输出
            StringBuilder resultBuilder = new StringBuilder();
            resultBuilder.append("===================================\n");
            resultBuilder.append("窗口结束时间：").append( new Timestamp(timestamp - 1)).append("\n");

            // 遍历列表，取top n输出
            for( int i = 0; i < Math.min(topSize, itemViewCounts.size()); i++ ){
                ItemViewCount currentItemViewCount = itemViewCounts.get(i);
                resultBuilder.append("NO ").append(i+1).append(":")
                        .append(" 单词 = ").append(currentItemViewCount.getWord())
                        .append(" 数量 = ").append(currentItemViewCount.getCount())
                        .append("\n");
            }
            resultBuilder.append("===============================\n\n");

            // 控制输出频率
            Thread.sleep(1000L);

            out.collect(resultBuilder.toString());
        }
    }

}
