package com.epam.domariev.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
@DefaultCoder(AvroCoder.class)
public class Activity implements Serializable {

    private String type;

    private int past7daysCount;

    private int past7daysUniqueCount;

    private int past30daysCount;

    private int past30daysUniqueCount;
}
