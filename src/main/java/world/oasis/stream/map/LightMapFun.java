package world.oasis.stream.map;

import com.vip.vjtools.vjkit.mapper.JsonMapper;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple6;


public class LightMapFun extends RichMapFunction<String, Tuple6<Integer, String, Integer, String, Double, Integer>> {


    @Override
    public Tuple6<Integer, String, Integer, String, Double, Integer> map(String s) {
        Tuple6<Integer, String, Integer, String, Double, Integer> obj = JsonMapper.INSTANCE.fromJson(s, Tuple6.class);
        return obj;
    }
}
