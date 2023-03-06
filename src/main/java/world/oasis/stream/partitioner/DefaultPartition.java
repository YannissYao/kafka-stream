package world.oasis.stream.partitioner;

import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;

import java.util.Random;

public class DefaultPartition extends FlinkKafkaPartitioner {

    private Random random = new Random(11);

    @Override
    public int partition(Object o, byte[] bytes, byte[] bytes1, String s, int[] ints) {
        return random.nextInt(ints.length);
    }
}
