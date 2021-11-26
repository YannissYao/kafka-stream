package world.oasis.stream.window;

import com.google.common.collect.Maps;
import com.vip.vjtools.vjkit.mapper.JsonMapper;
import com.vip.vjtools.vjkit.time.DateFormatUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Map;


@Slf4j
public class ProcessWindowFun extends ProcessWindowFunction<Tuple2<String, Integer>, String, String, TimeWindow> {

    @Override
    public void process(String s, Context context, Iterable<Tuple2<String, Integer>> iterable, Collector<String> collector) throws Exception {

        if (iterable.iterator().hasNext()) {
            Tuple2<String, Integer> t2 = iterable.iterator().next();
            Map<String, Object> map = Maps.newHashMapWithExpectedSize(10);
            map.put("key", t2.f0);
            map.put("aggVal", t2.f1);
            map.put("windowStartTime", DateFormatUtil.formatDate("yyyy/MM/dd HH:mm:ss", context.window().getStart()));
            map.put("windowEndTime", DateFormatUtil.formatDate("yyyy/MM/dd HH:mm:ss", context.window().getEnd()));
            log.info("aggResult====> {}", JsonMapper.INSTANCE.toJson(map));
            collector.collect(JsonMapper.INSTANCE.toJson(map));
        }
    }


}
