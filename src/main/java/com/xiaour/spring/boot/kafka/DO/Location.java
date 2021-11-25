package com.xiaour.spring.boot.kafka.DO;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Data
@Builder
public class Location {
    private Integer vehicleId;
    private String plate;
    private String color;
    private Integer date;
    private Integer gpsSpeed;
    private Integer limitSpeed;
    private Long devTime;

}

