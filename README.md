# Event statistics calculator app
Apache beam project, that calculate statistics of events and write it in AVRO format on GCS bucket. Statistics of each city will be written to separate file.
## How to run
1. Run `git clone git@github.com:DomarievRoman/event-statistics-aggregator-beam.git`
2. Run `mvn clean install`
3. Select runner
   - Direct runner `mvn compile exec:java -Dexec.mainClass=com.epam.domariev.pipeline.EventStatisticsCalculator -Dexec.args="--inputFiles=YOUR_GCS_INPUT_BUCKET/JSONL_FILES --      output=gs://YOUR_GCS_OUTPUT_BUCKET" -Pdirect-runner`
   - Dataflow runner `mvn compile exec:java -Dexec.mainClass=com.epam.domariev.pipeline.EventStatisticsCalculator -Dexec.args="--runner=DataflowRunner --gcpTempLocation=gs://YOUR_GCS_BUCKET/temp --project=YOUR_PROJECT --region=GCE_REGION --inputFiles=YOUR_GCS_INPUT_BUCKET/JSONL_FILES --output=YOUR_GCS_OUTPUT_BUCKET" -Pdataflow-runner`
