package com.xiaour.spring.boot.kafka.aggFun;

import com.xiaour.spring.boot.kafka.DO.Location;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class AggFun implements AggregateFunction<Location, Tuple2<String, Integer>, Tuple2<String, Integer>> {

    @Override
    public Tuple2<String, Integer> createAccumulator() {
        return new Tuple2<>("", 0);
    }

    @Override
    public Tuple2<String, Integer> add(Location location, Tuple2<String, Integer> stringIntegerTuple2) {
        System.out.println(location.getPlate() + "  " + stringIntegerTuple2.f1 + "   " + location.getGpsSpeed());
        stringIntegerTuple2.f1 += location.getGpsSpeed();
        return stringIntegerTuple2;
    }

    @Override
    public Tuple2<String, Integer> getResult(Tuple2<String, Integer> stringIntegerTuple2) {
        return stringIntegerTuple2;
    }

    @Override
    public Tuple2<String, Integer> merge(Tuple2<String, Integer> stringIntegerTuple2, Tuple2<String, Integer> acc1) {
        acc1.f1 += stringIntegerTuple2.f1;
        return stringIntegerTuple2;
    }
}
