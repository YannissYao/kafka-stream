package world.oasis.stream.aggFun;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import world.oasis.base.room.StreamEventEnum;

import java.util.Objects;

public class RoomLightAggFun implements AggregateFunction<Tuple6<Integer, String, Integer, String, Double, Integer>,
        Tuple4<String, Double, Long, Long>, Tuple4<String, Double, Long, Long>> {


    @Override
    public Tuple4<String, Double, Long, Long> createAccumulator() {
        return new Tuple4<>("", 0.0, 0L, 0L);
    }


    @Override
    public Tuple4<String, Double, Long, Long> add(Tuple6<Integer, String, Integer, String, Double, Integer> t6, Tuple4<String, Double, Long, Long> acc) {
        Integer eventId = t6.f0;
        if (Objects.isNull(eventId)) {
            return acc;
        }
        acc.f0 = t6.f1;
        if (Objects.equals(eventId, StreamEventEnum.ROOM_NUMBER_CHANGE_EVENT.getValue())) {
            acc.f1 += t6.f4;
        } else if (Objects.equals(eventId, StreamEventEnum.ROOM_FOLLOW_EVENT.getValue())) {
            acc.f1 += t6.f4;
        } else if (Objects.equals(eventId, StreamEventEnum.SEND_GIFT_EVENT.getValue())) {
            acc.f1 += t6.f4;
        } else if (Objects.equals(eventId, StreamEventEnum.IM_MSG_EVENT.getValue())) {
            acc.f1 += t6.f4;
        } else if (Objects.equals(eventId, StreamEventEnum.START_STOP_VOICE.getValue())) {
            acc.f1 += t6.f4;
        } else {
            return acc;
        }
        return acc;
    }

    @Override
    public Tuple4<String, Double, Long, Long> getResult(Tuple4<String, Double, Long, Long> t4) {
        return t4;
    }

    @Override
    public Tuple4<String, Double, Long, Long> merge(Tuple4<String, Double, Long, Long> t4, Tuple4<String, Double, Long, Long> acc1) {
        return null;
    }
}
