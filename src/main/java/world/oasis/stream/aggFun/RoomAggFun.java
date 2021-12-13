package world.oasis.stream.aggFun;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple6;
import world.oasis.base.room.StreamEventEnum;

import java.util.Objects;

public class RoomAggFun implements AggregateFunction<Tuple6<Integer, String, Long, String, Integer, Long>,
        Tuple10<String, Integer, String, String, String, String, Integer, Integer, Long, Long>,
        Tuple10<String, Integer, String, String, String, String, Integer, Integer, Long, Long>> {


    @Override
    public Tuple10<String, Integer, String, String, String, String, Integer, Integer, Long, Long> createAccumulator() {
        return new Tuple10<>("", 0, "", "", "", "", 0, 0, 0L, 0L);
    }

    @Override
    public Tuple10<String, Integer, String, String, String, String, Integer, Integer, Long, Long> add(Tuple6<Integer, String, Long, String, Integer, Long> newObj,
                                                                                                      Tuple10<String, Integer, String, String, String, String, Integer, Integer, Long, Long> acc) {
        Integer eventId = newObj.f0;
        if (Objects.isNull(eventId)) {
            return acc;
        }
        if (Objects.equals(eventId, StreamEventEnum.ROOM_NUMBER_CHANGE_EVENT.getValue())) {
            acc.f0 = newObj.f1;
            acc.f1 += newObj.f4;
            if (Objects.equals(1, newObj.f4)) {
                //加入房间Uid数组
                acc.f2 += "," + newObj.f2;
            } else {
                acc.f3 += "," + newObj.f2;
            }
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
