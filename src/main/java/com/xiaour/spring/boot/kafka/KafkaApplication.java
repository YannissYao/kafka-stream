package com.xiaour.spring.boot.kafka;


import com.vip.vjtools.vjkit.mapper.JsonMapper;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.Properties;


@EnableScheduling
@SpringBootApplication
public class KafkaApplication {


    public static void main(String[] args) {
        SpringApplication.run(KafkaApplication.class, args);
    }

    @Value("${spring.kafka.bootstrap-servers}")
    private String kafkaServer;

    @Bean
    public StreamExecutionEnvironment streamExecutionEnvironment() {

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


//        consumer.setStartFromEarliest(); // Flink从topic中最初的数据开始消费
        consumer.setCommitOffsetsOnCheckpoints(true);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);// 设置模式为exactly-once 默认(this is the default)
        env.enableCheckpointing(5000);//ms 执行间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);// 确保检查点之间有进行500 ms的进度
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(5);// 同一时间只允许进行一个检查点

//        AllWindowedStream allWindowedStream = env.addSource(consumer).windowAll(TumblingEventTimeWindows.of(Time.seconds(5)));

//        allWindowedStream.sum(0);


        env.addSource(consumer).map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return new Tuple2<>(s, Integer.valueOf(s));
            }
        }).keyBy(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(1)))//窗口大小
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t2, Tuple2<String, Integer> t1) throws Exception {
                        System.out.println(t1.f0 + "   " + (t1.f1 + t2.f1));
                        return new Tuple2<>(t1.f0, t1.f1 + t2.f1);
                    }
                })
                .map(new MapFunction<Tuple2<String, Integer>, String>() {
                    @Override
                    public String map(Tuple2<String, Integer> t2) throws Exception {
//                        System.out.println(JsonMapper.INSTANCE.toJson(t2));
                        return JsonMapper.INSTANCE.toJson(t2);
                    }
                })
                .addSink(new FlinkKafkaProducer<String>(
                        "localhost:9092",
                        "stream-out",
                        new SimpleStringSchema()

                ));
//                .print();


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return env;
    }
}
