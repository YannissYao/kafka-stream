package world.oasis.stream.map;

import com.vip.vjtools.vjkit.mapper.JsonMapper;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple6;


public class RoomMapFun extends RichMapFunction<String, Tuple6<Integer, String, Long, String, Integer, Long>> {


    @Override
    public Tuple6<Integer, String, Long, String, Integer, Long> map(String s) throws Exception {
        Tuple6<Integer, String, Long, String, Integer, Long> obj = JsonMapper.INSTANCE.fromJson(s, Tuple6.class);
        obj.f2 = Long.parseLong(String.valueOf(obj.f2));
        obj.f5 = Long.parseLong(String.valueOf(obj.f5));
        return obj;
    }

}
