package world.oasis.stream;


import com.vip.vjtools.vjkit.mapper.JsonMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import world.oasis.stream.DO.Location;
import world.oasis.stream.aggFun.AggFun;
import world.oasis.stream.window.ProcessWindowFun;

import java.util.Properties;

/**
 * Created by Yannis on 2021/11/24  20:55
 * /usr/local/Cellar/apache-flink/1.14.0/libexec/bin/flink run -c world.oasis.stream.StreamApplication /Users/Joeysin/IdeaWorkSpace/github/kafka-stream/target/kafka-stream-0.0.1-SNAPSHOT.jar --host localhost --port 8088
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
        env.enableCheckpointing(5000);//ms 执行间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);// 确保检查点之间有进行500 ms的进度
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);// 同一时间只允许进行一个检查点

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", args[0]);
        props.setProperty("group.id", "flink-group");


        //数据源配置，是一个kafka消息的消费者
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>("test2", new SimpleStringSchema(), props);
        FlinkKafkaProducer<String> flinkKafkaProducer = new FlinkKafkaProducer<String>(args[0], "stream-out", new SimpleStringSchema());
//        DO.setStartFromEarliest(); // Flink从topic中最初的数据开始消费
        consumer.setCommitOffsetsOnCheckpoints(true);
//        AllWindowedStream allWindowedStream = env.addSource(DO).windowAll(TumblingEventTimeWindows.of(Time.seconds(5)));
//        allWindowedStream.sum(0);
        DataStreamSource<String> streamSource = env.addSource(consumer);

        streamSource.map(new MapFunction<String, Location>() {
            @Override
            public Location map(String s) throws Exception {
                Location location = JsonMapper.INSTANCE.fromJson(s, Location.class);
//                System.out.println("====> " + JsonMapper.INSTANCE.toJson(location));
                return location;
            }
        })
//                .setParallelism(4)//并行数
                .keyBy(addSource -> addSource.getPlate())
//                //.countWindow(1)  //窗口填满1个开始计算
//                .window(GlobalWindows.create())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(20)))//窗口大小
                .aggregate(new AggFun(), new ProcessWindowFun())
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
