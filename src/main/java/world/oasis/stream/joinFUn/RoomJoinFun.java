package world.oasis.stream.joinFUn;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple10;

public class RoomJoinFun implements JoinFunction<Tuple10<String, Integer, String, String, String, String, String, Integer, Integer, Integer>, Tuple10<String, Integer, String, String, String, String, String, Integer, Integer, Integer>, Tuple10<String, Integer, String, String, String, String, String, Integer, Integer, Integer>> {

    @Override
    public Tuple10<String, Integer, String, String, String, String, String, Integer, Integer, Integer> join(Tuple10<String, Integer, String, String, String, String, String, Integer, Integer, Integer> l, Tuple10<String, Integer, String, String, String, String, String, Integer, Integer, Integer> r) throws Exception {

        return new Tuple10(l.f0, l.f1, l.f2, l.f3, l.f4, l.f5, l.f6, l.f7, l.f8, l.f9);
    }
}
