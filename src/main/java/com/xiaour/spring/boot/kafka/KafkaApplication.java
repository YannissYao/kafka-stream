package com.xiaour.spring.boot.kafka;


import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.apache.kafka.streams.StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG;


@EnableScheduling
@SpringBootApplication
public class KafkaApplication {

    private static final int MAX_MESSAGE_SIZE = 16 * 1024 * 1024;

    public static void main(String[] args) {
        SpringApplication.run(KafkaApplication.class, args);
    }


//    @Bean
//    public KStream<String, String> kStream(StreamsBuilder streamsBuilder) {
//        KStream<String, String> stream = streamsBuilder.stream("test");
//
//
//        // 有兴趣可以写个 wordCount
//        KTable kTable = stream.map((key, value) -> {
//            return new KeyValue<>(key, value);
//        }).groupByKey().count();
//
//
//        kTable.toStream().foreach((k, v) -> {
//            //=======business
//            System.out.println("K:" + k + "  V:" + v);
//        });
//        return stream;
//    }


    @Bean(destroyMethod = "close", initMethod = "start")
    public KafkaStreams kafkaStreams(KafkaProperties kafkaProperties) {
        final Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "oasis-stream");
//        props.put(StreamsConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG, 50);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 5000);//时间窗口
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//        props.put(StreamsConfig.STATE_DIR_CONFIG, "data");
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, JsonNode.class);
        props.put(DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //stream
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> stream = streamsBuilder.stream("test");


        KTable<String, String> sum = stream
                .groupByKey().reduce((x, y) -> {
//                    System.out.println("x: " + x + " " + "y: " + y);
                    //{changeCount , leaveuids[], joinUids[]}
                    Integer sum_s = Integer.valueOf(x) + Integer.valueOf(y);
//            System.out.println("sum: " + sum);
                    return sum_s.toString();
                });

        //创建一个消费者
        sum.toStream().map((x, y) -> {
//            System.out.println("K: " + x + "V: " + y);
            return new KeyValue<String, String>(x, y.toString());
        }).to("stream-out");


        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), props);
        return kafkaStreams;
    }
}
