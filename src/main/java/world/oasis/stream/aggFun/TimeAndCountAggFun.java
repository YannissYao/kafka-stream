package world.oasis.stream.aggFun;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class TimeAndCountAggFun implements AggregateFunction<String,
        Tuple3<String, Long, Long>, Tuple3<String, Long, Long>> {


    @Override
    public Tuple3<String, Long, Long> createAccumulator() {
        return new Tuple3<>("", 0L, 0L);
    }

    @Override
    public Tuple3<String, Long, Long> add(String s, Tuple3<String, Long, Long> stringLongLongTuple3) {
        stringLongLongTuple3.f0 += s + ",";
        return stringLongLongTuple3;
    }

    @Override
    public Tuple3<String, Long, Long> getResult(Tuple3<String, Long, Long> stringLongLongTuple3) {
        return stringLongLongTuple3;
    }

    @Override
    public Tuple3<String, Long, Long> merge(Tuple3<String, Long, Long> stringLongLongTuple3, Tuple3<String, Long, Long> acc1) {
        return null;
    }
}
