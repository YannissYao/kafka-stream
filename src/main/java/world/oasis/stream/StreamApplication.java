package world.oasis.stream;


import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import world.oasis.base.constant.AppConfig;
import world.oasis.stream.aggFun.CatRoomCountAggFun;
import world.oasis.stream.aggFun.ClubNumberCountAggFun;
import world.oasis.stream.aggFun.RoomAggFun;
import world.oasis.stream.aggFun.RoomLightAggFun;
import world.oasis.stream.map.CatCountMapFun;
import world.oasis.stream.map.ClubNumberCountMapFun;
import world.oasis.stream.map.RoomMapFun;
import world.oasis.stream.processwindow.CatRoomCountProcessWindowFun;
import world.oasis.stream.processwindow.ClubNumberCountProcessWindowFun;
import world.oasis.stream.processwindow.RoomProcessWindowFun;

import java.util.Properties;

/**
 * Created by Yannis on 2021/11/24  20:55
 * ./bin/flink run kafka-stream-0.0.1-SNAPSHOT.jar  -c world.oasis.stream.StreamApplication -p 1
 * ./bin/flink cancel {jobId}
 */

//@SpringBootApplication
@Slf4j
public class StreamApplication {


//    public static void main(String[] args) {
//        SpringApplication.run(StreamApplication.class, args);
//    }

//    @Value("${spring.kafka.bootstrap-servers}")
//    private String kafkaServer;
//https://gitbook.cn/books/5ebbd1623399900bec5fd93c/index.html      com.google.inject

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = new Properties();
        String address = System.getenv("KAFKA_ADDRESS");
        props.setProperty("bootstrap.servers", address);
        props.setProperty("group.id", "oasis-flink-group");
        //处理失败后重启策略
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
//                1, // 尝试重启的次数
//                Time.seconds(10)) // 间隔
//        );
//# run -e yarn-per-job -yjm 4096 -ytm 4096 --yarnslots 4  --detached /usr/lib/flink-current/examples/streaming/TopSpeedWindowing.jar
        //run -m yarn-cluster -yjm 1024 -ytm 2048 ossref://oasis-server-global/flink/oasis-flink-stream-1.0.0-SNAPSHOT.jar
//oasis-server-global/flink/oasis-flink-stream-1.0.0-SNAPSHOT.jar
        //https://help.aliyun.com/document_detail/85446.html    作业环境key
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);// 设置模式为exactly-once 默认(this is the default)
        // checkpoint执行有效期：要么1min完成 要么1min放弃
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.enableCheckpointing(10000);//ms chekpoint执行间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);// 确保检查点之间有进行500 ms的进度
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);// 同一时间只允许进行一个检查点
        // 表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //房间事件
        FlinkKafkaConsumer<String> roomEventConsumer = new FlinkKafkaConsumer(AppConfig.KAFKA_TOPIC_ROOM_EVENT_IN, new SimpleStringSchema(), props);
        FlinkKafkaProducer<String> roomEventOut = new FlinkKafkaProducer(address, AppConfig.KAFKA_TOPIC_ROOM_EVENT_OUT, new SimpleStringSchema());

        //曝光值
        FlinkKafkaConsumer<String> lightConsumer = new FlinkKafkaConsumer(AppConfig.KAFKA_TOPIC_ROOM_LIGHT_IN, new SimpleStringSchema(), props);
        FlinkKafkaProducer<String> lightOut = new FlinkKafkaProducer(address, AppConfig.KAFKA_TOPIC_ROOM_LIGHT_OUT, new SimpleStringSchema());

        //用户行为数量输出
//        FlinkKafkaProducer<String> userActionOut = new FlinkKafkaProducer<String>(address, AppConfig.KAFKA_TOPIC_USER_ACTION_OUT, new SimpleStringSchema());

        //分类下房间数输入/出
        FlinkKafkaConsumer<String> catRoomConsumer = new FlinkKafkaConsumer(AppConfig.KAFKA_TOPIC_CAT_COUNT_IN, new SimpleStringSchema(), props);
        FlinkKafkaProducer<String> catRoomCountOut = new FlinkKafkaProducer(address, AppConfig.KAFKA_TOPIC_CAT_COUNT_OUT, new SimpleStringSchema());

        //俱乐部成员数
        FlinkKafkaConsumer<String> clubNumberCountConsumer = new FlinkKafkaConsumer(AppConfig.KAFKA_TOPIC_CLUB_NUMBER_EVENT_IN, new SimpleStringSchema(), props);
        FlinkKafkaProducer<String> clubNumberCountOut = new FlinkKafkaProducer(address, AppConfig.KAFKA_TOPIC_CLUB_NUMBER_EVENT_OUT, new SimpleStringSchema());

//        DO.setStartFromEarliest(); // Flink从topic中最初的数据开始消费
        roomEventConsumer.setCommitOffsetsOnCheckpoints(true);
        lightConsumer.setCommitOffsetsOnCheckpoints(true);
        catRoomConsumer.setCommitOffsetsOnCheckpoints(true);
        clubNumberCountConsumer.setCommitOffsetsOnCheckpoints(true);
//        AllWindowedStream allWindowedStream = env.addSource(DO).windowAll(TumblingEventTimeWindows.of(Time.seconds(5)));
//        allWindowedStream.sum(0);
        DataStreamSource<String> roomEventStreamSource = env.addSource(roomEventConsumer);
        DataStreamSource<String> catCountStreamSource = env.addSource(catRoomConsumer);
        DataStreamSource<String> lightStreamSource = env.addSource(lightConsumer);
        DataStreamSource<String> clubNumberCountStreamSource = env.addSource(clubNumberCountConsumer);

        roomEventStreamSource
                .map(new RoomMapFun())
//                .union(hotStreamSource.map(new RichMapFunction<String, Tuple10<String, Integer, Long, String, String, String, String, Integer, Integer, Integer>>() {
//                    @Override
//                    public Tuple10<String, Integer, Long, String, String, String, String, Integer, Integer, Integer> map(String s) throws Exception {
//                        RoomEventDO roomEventDO = JsonMapper.INSTANCE.fromJson(s, RoomEventDO.class);
//                        return new Tuple10<>(roomEventDO.getRoomId(), 0, 0L, "", "", roomEventDO.getName(), "", 0, 0, 0);
//                    }
//                }))
//                .setParallelism(4)//并行数
                .keyBy(tuple6 -> tuple6.f1)
//                //.countWindow(1)  //窗口填满1个开始计算
//                .window(GlobalWindows.create())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))//窗口大小
//                .window(SlidingProcessingTimeWindows.of(Time.days(3),Time.seconds(10)))//窗口大小
                .aggregate(new RoomAggFun(), new RoomProcessWindowFun())
//                .print();
//                .writeAsText("/Users/Joeysin/Desktop/flink.txt");
                .addSink(roomEventOut).setParallelism(1);

        //曝光度 长分钟级别窗口
        lightStreamSource
                .map(new RoomMapFun())
                .keyBy(tuple6 -> tuple6.f1)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .aggregate(new RoomLightAggFun(), new RoomProcessWindowFun())
                .addSink(lightOut).setParallelism(1);

        //行为标签
//        lightStreamSource
//                .map(new RoomMapFun())
//                .keyBy(new KeySelector<Tuple6<Integer, String, Long, String, Integer, Long>, Tuple2<Integer, Long>>() {
//                    @Override
//                    public Tuple2<Integer, Long> getKey(Tuple6<Integer, String, Long, String, Integer, Long> tuple6) throws Exception {
//                        return Tuple2.of(tuple6.f0, tuple6.f2);
//                    }
//                })
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
//                .aggregate(new RoomActionTagAggFun(), new ActionTagProcessWindowFun())
//                .addSink(userActionOut).setParallelism(1);

        //分类下房间数统计
        catCountStreamSource
                .map(new CatCountMapFun())
                .keyBy(t2 -> t2.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
                .aggregate(new CatRoomCountAggFun(), new CatRoomCountProcessWindowFun())
                .addSink(catRoomCountOut).setParallelism(1);

        //俱乐部成员数统计
        clubNumberCountStreamSource
                .map(new ClubNumberCountMapFun())
                .keyBy(t2 -> t2.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
                .aggregate(new ClubNumberCountAggFun(), new ClubNumberCountProcessWindowFun())
                .addSink(clubNumberCountOut).setParallelism(1);
        try {
            env.execute("oasis-flink-stream-1.0.0");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
