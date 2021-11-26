package world.oasis.stream.DO;

import lombok.Data;

@Data
public class Location {
    private Integer vehicleId;
    private String plate;
    private String color;
    private Integer date;
    private Integer gpsSpeed;
    private Integer limitSpeed;
    private Long devTime;

}

