package com.epam.domariev.option;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface EventStatisticsOptions extends PipelineOptions {

    @Description("File path to read")
    @Default.String("gs://event-aggregator-input-bucket/*.jsonl")
    String getInputFiles();

    void setInputFiles(String value);

    @Description("File path to write")
    @Default.String("gs://event-aggregator-output-bucket")
    String getOutput();

    void setOutput(String value);

}
