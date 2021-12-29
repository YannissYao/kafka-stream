package world.oasis.stream.processwindow;

import com.vip.vjtools.vjkit.mapper.JsonMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


@Slf4j
public class LightValProcessWindowFun extends ProcessWindowFunction<Tuple4<String, Double, Long, Long>, String, String, TimeWindow> {


    @Override
    public void process(String s, ProcessWindowFunction<Tuple4<String, Double, Long, Long>, String, String, TimeWindow>.Context context, Iterable<Tuple4<String, Double, Long, Long>> iterable, Collector<String> collector) {
        if (iterable.iterator().hasNext()) {
            Tuple4<String, Double, Long, Long> t4 = iterable.iterator().next();
            t4.f2 = context.window().getStart();
            t4.f3 = context.window().getEnd();
            String json = JsonMapper.INSTANCE.toJson(t4);
            log.info("LightValResult====> {}", json);
            collector.collect(json);
        }
    }


}
