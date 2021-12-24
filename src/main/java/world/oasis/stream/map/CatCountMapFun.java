package world.oasis.stream.map;

import com.vip.vjtools.vjkit.mapper.JsonMapper;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;


public class CatCountMapFun extends RichMapFunction<String, Tuple2<Integer, Integer>> {


    @Override
    public Tuple2<Integer, Integer> map(String s) {
        Tuple2<Integer, Integer> t2 = JsonMapper.INSTANCE.fromJson(s, Tuple2.class);
        return t2;
    }
}
