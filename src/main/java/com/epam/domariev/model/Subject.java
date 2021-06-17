package com.epam.domariev.model;

import lombok.Data;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import java.io.Serializable;

@Data
@DefaultCoder(AvroCoder.class)
public class Subject implements Serializable {

    private String type;

    private String id;

}
