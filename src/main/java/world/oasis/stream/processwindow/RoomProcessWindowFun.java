package world.oasis.stream.processwindow;

import com.vip.vjtools.vjkit.mapper.JsonMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


@Slf4j
public class RoomProcessWindowFun extends ProcessWindowFunction<Tuple4<String, Integer, String, String>, String, String, TimeWindow> {

    @Override
    public void process(String s, Context context, Iterable<Tuple4<String, Integer, String, String>> iterable, Collector<String> collector) throws Exception {

        if (iterable.iterator().hasNext()) {
            Tuple4<String, Integer, String, String> t4 = iterable.iterator().next();
//            Map<String, Object> map = Maps.newHashMapWithExpectedSize(10);
//            map.put("key", t4.f0);
//            map.put("aggVal", t4.f1);
//            map.put("windowStartTime", DateFormatUtil.formatDate("yyyy/MM/dd HH:mm:ss", context.window().getStart()));
//            map.put("windowEndTime", DateFormatUtil.formatDate("yyyy/MM/dd HH:mm:ss", context.window().getEnd()));
            String json = JsonMapper.INSTANCE.toJson(t4);
            log.info("aggResult====> {}", json);
            collector.collect(json);
        }
    }
}
