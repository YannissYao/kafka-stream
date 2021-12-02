package world.oasis.stream.map;

import com.vip.vjtools.vjkit.mapper.JsonMapper;
import org.apache.flink.api.common.functions.RichMapFunction;
import world.oasis.stream.DO.Location;

public class RoomMapFun extends RichMapFunction<String, Location> {

//    private transient MapState<String, Integer> sum;


    @Override
    public Location map(String s) throws Exception {
        Location location = JsonMapper.INSTANCE.fromJson(s, Location.class);
//
//        Iterator<Map.Entry<String, Integer>> iterator = sum.iterator();
//        while (iterator.hasNext()) {
//            Map.Entry<String, Integer> next = iterator.next();
//            String key = next.getKey();
//            if (sum.contains(key)) {
//                sum.put(key, sum.get(key) + location.getGpsSpeed());
//            } else {
//                sum.put(key, location.getGpsSpeed());
//            }
//        }
        return location;
    }
//
//    public void open(Configuration parameters) throws Exception {
//
//        StateTtlConfig ttlConfig = StateTtlConfig
//                .newBuilder(org.apache.flink.api.common.time.Time.of(2, TimeUnit.SECONDS)) //这是state存活时间
//                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)//设置过期时间更新方式
//                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)//永远不要返回过期的状态
//                //.cleanupInRocksdbCompactFilter(1000)//处理完1000个状态查询时候，会启用一次CompactFilter
//                .build();
//
//        MapStateDescriptor<String, Integer> mapStateDescriptor = new MapStateDescriptor<>("mapStateDescriptor", String.class, Integer.class);
//        mapStateDescriptor.enableTimeToLive(ttlConfig);
//        sum = getRuntimeContext().getMapState(mapStateDescriptor);
//    }

}
