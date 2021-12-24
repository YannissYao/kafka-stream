package world.oasis.stream.map;

import com.vip.vjtools.vjkit.mapper.JsonMapper;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple6;


public class RoomMapFun extends RichMapFunction<String, Tuple6<Integer, String, Integer, String, Integer, Integer>> {


    @Override
    public Tuple6<Integer, String, Integer, String, Integer, Integer> map(String s) {
        Tuple6<Integer, String, Integer, String, Integer, Integer> obj = JsonMapper.INSTANCE.fromJson(s, Tuple6.class);
        return obj;
    }

}
