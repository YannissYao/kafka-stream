package com.xiaour.spring.boot.kafka.producer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.Random;
import java.util.UUID;

/**
 * @Author: Xiaour
 * @Description:
 * @Date: 2018/5/22 15:07
 */
@Component
public class Producer {

    @Autowired
    private KafkaTemplate kafkaTemplate;

    private static Gson gson = new GsonBuilder().create();

    //发送消息方法
    public void send() {

        Random random = new Random();
        int key = random.nextInt(2);
        Integer[] arr = {100, -100};
        kafkaTemplate.send("test", String.valueOf(key), String.valueOf(arr[key]));

        kafkaTemplate.send("test2", String.valueOf(key), String.valueOf(arr[key]));
    }

}
