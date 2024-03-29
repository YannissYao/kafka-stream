package world.oasis.stream.trigger;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;

/**
 * CountAndTimeTrigger : 满足一定条数和时间触发
 * 条数的触发使用计数器计数
 * 时间的触发，使用 flink 的 timerServer，注册触发器触发
 *
 * @param <W>
 */
@Slf4j
@Data
public class CountOrTimeTrigger<W extends Window> extends Trigger<Object, W> {
    // 触发的条数
    private final long size;
    // 触发的时长
    private final long interval;
    private static final long serialVersionUID = 1L;
    // 条数计数器
    private final ReducingStateDescriptor<Long> countStateDesc =
            new ReducingStateDescriptor<>("count", new ReduceSum(), LongSerializer.INSTANCE);
    // 时间计数器，保存下一次触发的时间
    private final ReducingStateDescriptor<Long> timeStateDesc =
            new ReducingStateDescriptor<>("fire-interval", new ReduceMin(), LongSerializer.INSTANCE);


    @Override
    public TriggerResult onElement(Object element, long timestamp, W window, TriggerContext ctx) throws Exception {
        // 注册窗口结束的触发器, 不需要会自动触发
//        ctx.registerProcessingTimeTimer(window.maxTimestamp());
        // count
        ReducingState<Long> count = ctx.getPartitionedState(countStateDesc);
        //interval
        ReducingState<Long> fireTimestamp = ctx.getPartitionedState(timeStateDesc);
        // 每条数据 counter + 1
        count.add(1L);
        if (count.get() >= size) {
            log.info("countTrigger triggered, count : {}", count.get());
            // 满足条数的触发条件，先清 0 条数计数器
            count.clear();
            // 满足条数时也需要清除时间的触发器，如果不是创建结束的触发器
            if (fireTimestamp.get() != window.maxTimestamp()) {
//                logger.info("delete trigger : {}, {}", sdf.format(fireTimestamp.get()), fireTimestamp.get());
                ctx.deleteProcessingTimeTimer(fireTimestamp.get());
            }
            fireTimestamp.clear();
            // fire 触发计算
            return TriggerResult.FIRE;
        }

        // 触发之后，下一条数据进来才设置时间计数器注册下一次触发的时间
        timestamp = ctx.getCurrentProcessingTime();
        if (fireTimestamp.get() == null) {
//            long start = timestamp - (timestamp % interval);
            long nextFireTimestamp = timestamp + interval;
//            logger.info("register trigger : {}, {}", sdf.format(nextFireTimestamp), nextFireTimestamp);
            ctx.registerProcessingTimeTimer(nextFireTimestamp);
            fireTimestamp.add(nextFireTimestamp);
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception {

        // count
        ReducingState<Long> count = ctx.getPartitionedState(countStateDesc);
        //interval
        ReducingState<Long> fireTimestamp = ctx.getPartitionedState(timeStateDesc);

        // time trigger and window end
        if (time == window.maxTimestamp()) {
            log.info("window close : {}", time);
            // 窗口结束，清0条数和时间的计数器
            count.clear();
            ctx.deleteProcessingTimeTimer(fireTimestamp.get());
            fireTimestamp.clear();
            return TriggerResult.FIRE_AND_PURGE;
        } else if (fireTimestamp.get() != null && fireTimestamp.get().equals(time)) {
            log.info("timeTrigger trigger, time : {}", time);
            // 时间计数器触发，清0条数和时间计数器
            count.clear();
            fireTimestamp.clear();
            return TriggerResult.FIRE;
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long l, W w, TriggerContext triggerContext) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(W w, TriggerContext triggerContext) throws Exception {

    }

    /**
     * 用于合并
     */
    private static class ReduceSum implements ReduceFunction<Long> {
        private static final long serialVersionUID = 1L;

        @Override
        public Long reduce(Long value1, Long value2) {
            return value1 + value2;
        }
    }


    private static class ReduceMin implements ReduceFunction<Long> {
        private static final long serialVersionUID = 1L;

        @Override
        public Long reduce(Long value1, Long value2) {
            return Math.min(value1, value2);
        }
    }
}
