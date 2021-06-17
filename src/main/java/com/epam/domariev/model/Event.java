package com.epam.domariev.model;

import lombok.Data;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

import java.io.Serializable;

@Data
@DefaultSchema(JavaBeanSchema.class)
public class Event implements Serializable {

    private String id;

    private String userId;

    private String city;

    private String eventType;

    private Long timestamp;

    private Subject eventSubject;
}
