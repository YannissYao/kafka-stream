package world.oasis.stream.DO;


import lombok.Data;
import me.j360.framework.base.domain.BaseDO;

@Data
public class RoomEventDO extends BaseDO {

    private Long id;
    private String roomId;
    private String groupId;
    private Long uid;
    ///0=语音，1=好友
    private Integer type;
    private Integer event;
    private Long startTime;
    private Long stopTime;


}
