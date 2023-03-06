package world.oasis.stream.processwindow;

import com.vip.vjtools.vjkit.mapper.JsonMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;


@Slf4j
public class TimeAndCountProcessWindowFun implements AllWindowFunction<Tuple3<String, Long, Long>, String, GlobalWindow> {


    @Override
    public void apply(GlobalWindow globalWindow, Iterable<Tuple3<String, Long, Long>> iterable, Collector<String> collector) throws Exception {
        if (iterable.iterator().hasNext()) {
            Tuple3<String, Long, Long> t3 = iterable.iterator().next();
            String json = JsonMapper.INSTANCE.toJson(t3);
            log.info("RoomEventResult====> {}", json);
            collector.collect(json);
        }
    }
}
