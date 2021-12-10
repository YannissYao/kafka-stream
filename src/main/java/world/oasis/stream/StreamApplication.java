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
import world.oasis.stream.aggFun.RoomAggFun;
import world.oasis.stream.map.RoomMapFun;
import world.oasis.stream.processwindow.RoomProcessWindowFun;

import java.util.Properties;

/**
 * Created by Yannis on 2021/11/24  20:55
 * ./bin/flink run kafka-stream-0.0.1-SNAPSHOT.jar  -c指定入口类 -p 指定并行度
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

        //处理失败后重启策略
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
//                1, // 尝试重启的次数
//                Time.seconds(10)) // 间隔
//        );


        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);// 设置模式为exactly-once 默认(this is the default)
        env.enableCheckpointing(10000);//ms chekpoint执行间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);// 确保检查点之间有进行500 ms的进度
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);// 同一时间只允许进行一个检查点
        // 表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", args[0]);
        props.setProperty("group.id", "oasis-flink-group");


        //数据源配置，是一个kafka消息的消费者
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>(AppConfig.KAFKA_TOPIC_ROOM_IN, new SimpleStringSchema(), props);
        FlinkKafkaProducer<String> flinkKafkaProducer = new FlinkKafkaProducer<String>(args[0], AppConfig.KAFKA_TOPIC_ROOM_OUT, new SimpleStringSchema());
//        DO.setStartFromEarliest(); // Flink从topic中最初的数据开始消费
        consumer.setCommitOffsetsOnCheckpoints(true);
//        AllWindowedStream allWindowedStream = env.addSource(DO).windowAll(TumblingEventTimeWindows.of(Time.seconds(5)));
//        allWindowedStream.sum(0);
        DataStreamSource<String> streamSource = env.addSource(consumer);

        streamSource.map(new RoomMapFun())
//                .setParallelism(4)//并行数
                .keyBy(roomEvent -> roomEvent.getRoomId())
//                //.countWindow(1)  //窗口填满1个开始计算
//                .window(GlobalWindows.create())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))//窗口大小
//                .window(SlidingProcessingTimeWindows.of(Time.days(3),Time.seconds(10)))//窗口大小
                .aggregate(new RoomAggFun(), new RoomProcessWindowFun())
//                .print();
//                .writeAsText("/Users/Joeysin/Desktop/flink.txt");
                .addSink(flinkKafkaProducer);
        try {
            env.execute("oasis-flink-stream-1.0.0");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


//    @Override
//    public void run(String... args) throws Exception {


//    }
}
