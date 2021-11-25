package com.xiaour.spring.boot.kafka.window;

import com.vip.vjtools.vjkit.mapper.JsonMapper;
import com.vip.vjtools.vjkit.time.DateFormatUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


//TODO StateTtlConfig

public class ProcessWindowFun extends ProcessWindowFunction<Tuple2<String, Integer>, String, String, TimeWindow> {

    @Override
    public void process(String s, Context context, Iterable<Tuple2<String, Integer>> iterable, Collector<String> collector) throws Exception {

        if (iterable.iterator().hasNext()) {
            Tuple2<String, Integer> t2 = iterable.iterator().next();

            Tuple4 tuple4 = new Tuple4<>(t2.f0, t2.f1, DateFormatUtil.formatDate("yyyy/MM/dd HH:mm:ss", context.window().getStart()),
                    DateFormatUtil.formatDate("yyyy/MM/dd HH:mm:ss", context.window().getEnd()));
            collector.collect(JsonMapper.INSTANCE.toJson(tuple4));
        }
    }


}
