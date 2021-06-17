package com.epam.domariev.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class EventSubjectActivity implements Serializable {

    Map<Subject, List<Activity>> subjects;
}
