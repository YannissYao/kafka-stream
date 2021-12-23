package world.oasis.stream.aggFun;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple6;
import world.oasis.base.room.StreamEventEnum;

import java.util.Objects;
import java.util.Optional;

public class RoomActionTagAggFun implements AggregateFunction<Tuple6<Integer, String, Long, String, Integer, Long>,
        Tuple10<String, Integer, String, String, String, String, Integer, Integer, Long, Long>,
        Tuple10<String, Integer, String, String, String, String, Integer, Integer, Long, Long>> {


    @Override
    public Tuple10<String, Integer, String, String, String, String, Integer, Integer, Long, Long> createAccumulator() {
        return new Tuple10<>("", 0, "", "", "", "", 0, 0, 0L, 0L);
    }

    @Override
    public Tuple10<String, Integer, String, String, String, String, Integer, Integer, Long, Long> add(Tuple6<Integer, String, Long, String, Integer, Long> t6,

                                                                                                      Tuple10<String, Integer, String, String, String, String, Integer, Integer, Long, Long> acc) {
        if (Objects.equals(StreamEventEnum.ROOM_NUMBER_CHANGE_EVENT.getValue(), t6.f0)) {
            return acc;
        }
        //用户标签count++
        acc.f0 = "action";
        acc.f1 = Optional.ofNullable(t6.f0).orElse(0);//StreamEventEnum  f3 走标签
        acc.f6 = Optional.ofNullable(t6.f2).orElse(0L).intValue();//uid
        acc.f7 += Optional.ofNullable(t6.f5).orElse(0L).intValue();//count
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
