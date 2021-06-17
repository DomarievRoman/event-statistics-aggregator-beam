package com.epam.domariev.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
@DefaultSchema(JavaBeanSchema.class)
public class EventInfo implements Serializable {

    private String id;

    private String userId;

    private String city;

    private String eventType;

    private Long timestamp;
}
