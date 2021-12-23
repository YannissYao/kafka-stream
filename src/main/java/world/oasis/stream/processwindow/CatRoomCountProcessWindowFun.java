package world.oasis.stream.processwindow;

import com.vip.vjtools.vjkit.mapper.JsonMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


@Slf4j
public class CatRoomCountProcessWindowFun extends ProcessWindowFunction<Tuple10<String, Integer, String, String, String, String, Integer, Integer, Long, Long>, String,Long, TimeWindow> {


    @Override
    public void process(Long aLong, ProcessWindowFunction<Tuple10<String, Integer, String, String, String, String, Integer, Integer, Long, Long>, String, Long, TimeWindow>.Context context, Iterable<Tuple10<String, Integer, String, String, String, String, Integer, Integer, Long, Long>> iterable, Collector<String> collector) throws Exception {
        if (iterable.iterator().hasNext()) {
            Tuple10<String, Integer, String, String, String, String, Integer, Integer, Long, Long> t10 = iterable.iterator().next();
            t10.f8 = context.window().getStart();
            t10.f9 = context.window().getEnd();
            String json = JsonMapper.INSTANCE.toJson(t10);
            log.info("catRoomCountResult====> {}", json);
            collector.collect(json);
        }
    }
}
