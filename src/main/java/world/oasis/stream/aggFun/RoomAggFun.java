package world.oasis.stream.aggFun;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import world.oasis.stream.DO.RoomEventDO;

import java.util.Objects;

public class RoomAggFun implements AggregateFunction<RoomEventDO, Tuple4<String, Integer, String, String>, Tuple4<String, Integer, String, String>> {


    @Override
    public Tuple4<String, Integer, String, String> createAccumulator() {
        return new Tuple4<>("", 0, "", "");
    }

    @Override
    public Tuple4<String, Integer, String, String> add(RoomEventDO roomEventDO, Tuple4<String, Integer, String, String> t4) {
        t4.f0 = roomEventDO.getRoomId();
        t4.f1 += roomEventDO.getEvent();
        if (Objects.equals(1, roomEventDO.getEvent())) {
            //加入房间Uid数组
            t4.f2 += "," + roomEventDO.getUid();
        } else {
            t4.f3+= "," + roomEventDO.getUid();
        }
        return t4;
    }

    @Override
    public Tuple4<String, Integer, String, String> getResult(Tuple4<String, Integer, String, String> stringIntegerListListTuple4) {
        return stringIntegerListListTuple4;
    }

    @Override
    public Tuple4<String, Integer, String, String> merge(Tuple4<String, Integer, String, String> stringIntegerListListTuple4, Tuple4<String, Integer, String, String> acc1) {
        return null;
    }
}
