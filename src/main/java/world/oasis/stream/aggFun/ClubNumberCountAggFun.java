package world.oasis.stream.aggFun;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class ClubNumberCountAggFun implements AggregateFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>> {

    @Override
    public Tuple2<String, Integer> createAccumulator() {
        return new Tuple2<>("", 0);
    }

    @Override
    public Tuple2<String, Integer> add(Tuple2<String, Integer> t2, Tuple2<String, Integer> acc) {
        acc.f0 = t2.f0;//clubId
        acc.f1 += t2.f1;//count
        return acc;
    }

    @Override
    public Tuple2<String, Integer> getResult(Tuple2<String, Integer> stringIntegerTuple2) {
        return stringIntegerTuple2;
    }

    @Override
    public Tuple2<String, Integer> merge(Tuple2<String, Integer> stringIntegerTuple2, Tuple2<String, Integer> acc1) {
        return null;
    }
}
