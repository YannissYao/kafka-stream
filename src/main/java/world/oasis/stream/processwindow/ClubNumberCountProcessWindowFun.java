package world.oasis.stream.processwindow;

import com.vip.vjtools.vjkit.mapper.JsonMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


@Slf4j
public class ClubNumberCountProcessWindowFun extends ProcessWindowFunction<Tuple2<String, Integer>, String, String, TimeWindow> {


    @Override
    public void process(String s, ProcessWindowFunction<Tuple2<String, Integer>, String, String, TimeWindow>.Context context, Iterable<Tuple2<String, Integer>> iterable, Collector<String> collector) {
        if (iterable.iterator().hasNext()) {
            Tuple2<String, Integer> t2 = iterable.iterator().next();
            String json = JsonMapper.INSTANCE.toJson(t2);
            log.info("ClubNumberCountResult====> {}", json);
            collector.collect(json);
        }
    }
}
