package com.xiaour.spring.boot.kafka;


import com.vip.vjtools.vjkit.mapper.JsonMapper;
import com.xiaour.spring.boot.kafka.DO.Location;
import com.xiaour.spring.boot.kafka.aggFun.AggFun;
import com.xiaour.spring.boot.kafka.window.ProcessWindowFun;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Properties;


/**
 * Created by Yannis on 2021/11/24  20:55
 * /usr/local/Cellar/apache-flink/1.14.0/libexec/bin/flink run -c com.xiaour.spring.boot.kafk.KafkaApplication /Users/Joeysin/IdeaWorkSpace/github/kafka-stream/target/kafka-stream-0.0.1-SNAPSHOT.jar --hostname localhost --port 8088
 */

@SpringBootApplication
public class KafkaApplication implements CommandLineRunner {


    public static void main(String[] args) {
        SpringApplication.run(KafkaApplication.class, args);
    }

    @Value("${spring.kafka.bootstrap-servers}")
    private String kafkaServer;


    @Override
    public void run(String... args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //处理失败后重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                8, // 尝试重启的次数
                org.apache.flink.api.common.time.Time.seconds(10)) // 间隔
        );


        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafkaServer);
        props.setProperty("group.id", "flink-group");

        //数据源配置，是一个kafka消息的消费者
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>("test2", new SimpleStringSchema(), props);


//        DO.setStartFromEarliest(); // Flink从topic中最初的数据开始消费
        consumer.setCommitOffsetsOnCheckpoints(true);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);// 设置模式为exactly-once 默认(this is the default)
        env.enableCheckpointing(5000);//ms 执行间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);// 确保检查点之间有进行500 ms的进度
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(5);// 同一时间只允许进行一个检查点

//        AllWindowedStream allWindowedStream = env.addSource(DO).windowAll(TumblingEventTimeWindows.of(Time.seconds(5)));

//        allWindowedStream.sum(0);


        DataStreamSource<String> streamSource = env.addSource(consumer);

        streamSource.map(new MapFunction<String, Location>() {
            @Override
            public Location map(String s) throws Exception {
                Location location = JsonMapper.INSTANCE.fromJson(s, Location.class);
                return location;
            }
        })
//                .setParallelism(4)//并行数
                .keyBy(a -> a.getPlate())
                //.countWindow(1)  //窗口填满1个开始计算
                //.window(GlobalWindows.create())
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))//窗口大小
                .aggregate(new AggFun(), new ProcessWindowFun())
//                .print();
                .addSink(new FlinkKafkaProducer<String>(
                        "localhost:9092",
                        "stream-out",
                        new SimpleStringSchema()

                ));


//                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
//            @Override
//            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t2, Tuple2<String, Integer> t1) throws Exception {
//                System.out.println(t1.f0 + "   " + (t1.f1 + t2.f1));
//                return new Tuple2<>(t1.f0, t1.f1 + t2.f1);
//            }
//        })
//                .map(new MapFunction<Tuple2<String, Integer>, String>() {
//                    @Override
//                    public String map(Tuple2<String, Integer> t2) throws Exception {
////                        System.out.println(JsonMapper.INSTANCE.toJson(t2));
//                        return JsonMapper.INSTANCE.toJson(t2);
//                    }
//                })
//                .addSink(new FlinkKafkaProducer<String>(
//                        "localhost:9092",
//                        "stream-out",
//                        new SimpleStringSchema()
//
//                ));
//                .print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
