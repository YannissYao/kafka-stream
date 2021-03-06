package world.oasis.stream.aggFun;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Objects;

public class CatRoomCountAggFun implements AggregateFunction<Tuple2<Integer, Integer>, Tuple10<String, Integer, String, String, String, String, Integer, Integer, Long, Long>, Tuple10<String, Integer, String, String, String, String, Integer, Integer, Long, Long>> {


    @Override
    public Tuple10<String, Integer, String, String, String, String, Integer, Integer, Long, Long> createAccumulator() {
        return new Tuple10<>("", 0, "", "", "", "", 0, 0, 0L, 0L);
    }

    @Override
    public Tuple10<String, Integer, String, String, String, String, Integer, Integer, Long, Long> add(Tuple2<Integer, Integer> t2,
                                                                                                      Tuple10<String, Integer, String, String, String, String, Integer, Integer, Long, Long> acc) {
        if (Objects.equals(0, t2.f0)) {
            return acc;
        }
        acc.f1 = t2.f0;//catId
        acc.f6 += t2.f1;//count
        return acc;
    }

    @Override
    public Tuple10<String, Integer, String, String, String, String, Integer, Integer, Long, Long> getResult(Tuple10<String, Integer, String, String, String, String, Integer, Integer, Long, Long> t10) {
        return t10;
    }

    @Override
    public Tuple10<String, Integer, String, String, String, String, Integer, Integer, Long, Long> merge(Tuple10<String, Integer, String, String, String, String, Integer, Integer, Long, Long> t10, Tuple10<String, Integer, String, String, String, String, Integer, Integer, Long, Long> acc1) {
        return null;
    }
}
