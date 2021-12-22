package world.oasis.stream.aggFun;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple6;
import world.oasis.base.room.StreamEventEnum;

import java.util.Objects;

public class RoomLightAggFun implements AggregateFunction<Tuple6<Integer, String, Long, String, Integer, Long>,
        Tuple10<String, Integer, String, String, String, String, Integer, Integer, Long, Long>,
        Tuple10<String, Integer, String, String, String, String, Integer, Integer, Long, Long>> {


    @Override
    public Tuple10<String, Integer, String, String, String, String, Integer, Integer, Long, Long> createAccumulator() {
        return new Tuple10<>("", 0, "", "", "", "", 0, 0, 0L, 0L);
    }

    @Override
    public Tuple10<String, Integer, String, String, String, String, Integer, Integer, Long, Long> add(Tuple6<Integer, String, Long, String, Integer, Long> t6,
                                                                                                      Tuple10<String, Integer, String, String, String, String, Integer, Integer, Long, Long> acc) {
        Integer eventId = t6.f0;
        if (Objects.isNull(eventId)) {
            return acc;
        }
        acc.f0 = t6.f1;
        if (Objects.equals(eventId, StreamEventEnum.ROOM_NUMBER_CHANGE_EVENT.getValue())) {
            acc.f6 += t6.f4;
        }else if(Objects.equals(eventId, StreamEventEnum.ROOM_FOLLOW_EVENT.getValue())){
            acc.f6 += t6.f4;
        }else if(Objects.equals(eventId, StreamEventEnum.SEND_GIFT_EVENT.getValue())){
            acc.f6 += t6.f4;
        }else if(Objects.equals(eventId, StreamEventEnum.IM_MSG_EVENT.getValue())){
            acc.f6 += t6.f4;
        }else if(Objects.equals(eventId, StreamEventEnum.START_STOP_VOICE.getValue())){
            acc.f6 += t6.f4;
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
