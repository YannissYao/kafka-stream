package com.xiaour.spring.boot.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * @Author: Xiaour
 * @Description:
 * @Date: 2018/5/22 15:03
 */
@Component
public class Consumer {

    @KafkaListener(topics = {"test"})
    public void listen(ConsumerRecord record) {

        Optional<?> kafkaMessage = Optional.ofNullable(record.value());

        if (kafkaMessage.isPresent()) {

            Object message = kafkaMessage.get();
//            System.out.println("------------->" + record);
//            System.out.println("----------->" + message);

        }


    }

    @KafkaListener(topics = {"stream-out"})
    public void listen2(ConsumerRecord<?, ?> record) {

        Optional<?> kafkaMessage = Optional.ofNullable(record.value());

        if (kafkaMessage.isPresent()) {

            Object message = kafkaMessage.get();
//            System.out.println("======>"+record);

            System.out.println("=======> K: " + record.key() + " V: " + message);

        }

    }

    @KafkaListener(topics = {"stream-out2"})
    public void listen3(ConsumerRecord<?, ?> record) {

        Optional<?> kafkaMessage = Optional.ofNullable(record.value());

        if (kafkaMessage.isPresent()) {

            Object message = kafkaMessage.get();
//            System.out.println("======>"+record);

            System.out.println("------> K: " + record.key() + " V: " + message);

        }

    }
}
