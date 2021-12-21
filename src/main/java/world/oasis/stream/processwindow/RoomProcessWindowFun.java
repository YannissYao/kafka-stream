package world.oasis.stream.processwindow;

import com.vip.vjtools.vjkit.mapper.JsonMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


@Slf4j
public class RoomProcessWindowFun extends ProcessWindowFunction<Tuple10<String, Integer, String, String, String, String, Integer, Integer, Long, Long>, String, String, TimeWindow> {

    @Override
    public void process(String s, Context context, Iterable<Tuple10<String, Integer, String, String, String, String, Integer, Integer, Long, Long>> iterable, Collector<String> collector) throws Exception {

        if (iterable.iterator().hasNext()) {
            Tuple10<String, Integer, String, String, String, String, Integer, Integer, Long, Long> t10 = iterable.iterator().next();
//            Map<String, Object> map = Maps.newHashMapWithExpectedSize(10);
//            map.put("key", t4.f0);
//            map.put("aggVal", t4.f1);
//            map.put("windowStartTime", DateFormatUtil.formatDate("yyyy/MM/dd HH:mm:ss", context.window().getStart()));
//            map.put("windowEndTime", DateFormatUtil.formatDate("yyyy/MM/dd HH:mm:ss", context.window().getEnd()));
            t10.f8 = context.window().getStart();
            t10.f9 = context.window().getEnd();
            String json = JsonMapper.INSTANCE.toJson(t10);
            log.info("aggLightResult====> {}", json);
            collector.collect(json);
        }
    }
}
