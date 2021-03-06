package world.oasis.stream.aggFun;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple6;
import world.oasis.base.room.StreamEventEnum;

import java.util.Objects;

public class RoomAggFun implements AggregateFunction<Tuple6<Integer, String, Integer, String, Integer, Integer>,
        Tuple10<String, Integer, String, String, String, String, Integer, Integer, Long, Long>,
        Tuple10<String, Integer, String, String, String, String, Integer, Integer, Long, Long>> {


    @Override
    public Tuple10<String, Integer, String, String, String, String, Integer, Integer, Long, Long> createAccumulator() {
        return new Tuple10<>("", 0, "", "", "", "", 0, 0, 0L, 0L);
    }

    @Override
    public Tuple10<String, Integer, String, String, String, String, Integer, Integer, Long, Long> add(Tuple6<Integer, String, Integer, String, Integer, Integer> t6,
                                                                                                      Tuple10<String, Integer, String, String, String, String, Integer, Integer, Long, Long> acc) {
        Integer eventId = Integer.parseInt(String.valueOf(t6.f0));
        if (Objects.isNull(eventId)) {
            return acc;
        }
        acc.f0 = String.valueOf(t6.f1);
        if (Objects.equals(eventId, StreamEventEnum.ROOM_NUMBER_CHANGE_EVENT.getValue())) {
            acc.f1 += t6.f4;
            if (Objects.equals(1, t6.f4)) {
                //加入房间Uid数组
                acc.f2 += "," + t6.f2;
            } else if (Objects.equals(-1, t6.f4)) {
                acc.f3 += "," + t6.f2;
            }
        } else if (Objects.equals(eventId, StreamEventEnum.QUERY_ROOM_EVENT.getValue())) {
            acc.f6 += t6.f4;
        } else {
            return acc;
        }
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
