package com.epam.domariev.pipeline;

import com.epam.domariev.model.*;
import com.epam.domariev.option.EventStatisticsOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.extensions.jackson.ParseJsons;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;


public class EventStatisticsCalculator {

    public static void main(String[] args) {
        EventStatisticsOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation().as(EventStatisticsOptions.class);
        runEventStatisticsCalculator(options);
    }

    public static void runEventStatisticsCalculator(EventStatisticsOptions options) {
        Pipeline pipeline = Pipeline.create(options);

        CoderRegistry coderRegistry = pipeline.getCoderRegistry();
        coderRegistry.registerCoderForClass(Subject.class, AvroCoder.of(Subject.class));

        /**
         * Read .jsonl files from GCP bucket and parse it to POJO
         * Use SerializableCoder
         */
        PCollection<Event> events = pipeline
                .apply("Read .jsonl", TextIO.read().from(options.getInputFiles()))
                .apply("Parse json to Event POJO", ParseJsons.of(Event.class))
                .setCoder(SerializableCoder.of(Event.class));
        /**
         * Form pairs of City and Event via {@link GenerateKVCityEventFn}
         * Use KVCoder of StringUtf8Coder and SerializableCoder
         */
        PCollection<KV<String, Event>> cityEvents = events
                .apply("Generate city (string):event (event) pairs",
                        ParDo.of(new GenerateKVCityEventFn()))
                .setCoder(KvCoder.of(
                        StringUtf8Coder.of(),
                        SerializableCoder.of(Event.class)
                ));
        /**
         * Group each city by their Event via GroupByKey - return Iterable of Event
         */
        PCollection<KV<String, Iterable<Event>>> groupedCitiesByEvents = cityEvents
                .apply("Group events by cities", GroupByKey.create());
        /**
         * Divide Event into {@link Subject} and {@link EventInfo} via {@link DivideEventFn}
         * Subject contains type of writing (article, instruction, guide etc.) and it's id
         * EventInfo contains type of action (comment, share etc.), that performs user on each writing, id of record,
         * user id, city and timestamp
         */
        PCollection<KV<String, List<KV<Subject, EventInfo>>>> dividedSubjectInfo = groupedCitiesByEvents
                .apply("Divide Event into Subject and EventInfo",
                        ParDo.of(new DivideEventFn()));
        /**
         * Next 3 steps - group each EventInfo by Subject via {@link GetListOfPairsFn}, {@link GetSubjectEventPairsFn}
         * and final method GroupByKey
         */
        PCollection<List<KV<Subject, EventInfo>>> listPCollection = dividedSubjectInfo
                .apply("Get List of pairs Subject & EventInfo",
                        ParDo.of(new GetListOfPairsFn()));

        PCollection<KV<Subject, EventInfo>> pCollection = listPCollection
                .apply("Get pairs Subject and EventInfo",
                        ParDo.of(new GetSubjectEventPairsFn()));

        PCollection<KV<Subject, Iterable<EventInfo>>> kvpCollection = pCollection
                .apply("Group Event Info by Subject", GroupByKey.create());
        /**
         * Convert the result of GroupByKey method to List via {@link IterableToListFn}
         */
        PCollection<KV<Subject, List<EventInfo>>> iterableToList = kvpCollection
                .apply("Convert Iterable to List",
                        ParDo.of(new IterableToListFn()));
        /**
         * Group Subject : List<EventInfo> by city via {@link KVToListFn} and GroupByKey method
         */
        PCollection<KV<String, KV<Subject, List<EventInfo>>>> cityGroupedEventInfo = iterableToList
                .apply("Convert KV to List<KV>",
                        ParDo.of(new KVToListFn()));

        PCollection<KV<String, Iterable<KV<Subject, List<EventInfo>>>>> groupedAll = cityGroupedEventInfo
                .apply("Group Subject & EventInfo by city", GroupByKey.create());

        PCollection<KV<String, List<KV<Subject, List<EventInfo>>>>> subjectEventInfoKV = groupedAll
                .apply("Convert Iterable to List",
                        ParDo.of(new IterableEventToListFn()));
        /**
         * Create statistics of each Subject {@link Activity} via {@link EventInfoToActivityTransformFn}
         * Activity contains information about type of user's action, count of past 7 and 30 days user action and
         * unique one by same amount of days (uniqueness by user id)
         */
        PCollection<KV<String, Map<Subject, List<Activity>>>> subjectActivityKV = subjectEventInfoKV
                .apply("Create Event statistics grouped by cities",
                        ParDo.of(new EventInfoToActivityTransformFn()));
        /**
         * Map statistics into {@link EventSubjectActivity} via {@link MapToEventSubjectActivityFn}
         * Use KVCoder of StringUtf8Coder and SerializableCoder
         */
        PCollection<KV<String, EventSubjectActivity>> eventSubjectActivity = subjectActivityKV
                .apply("Parse Map to EventSubjectActivity", ParDo.of(new MapToEventSubjectActivityFn()))
                .setCoder(KvCoder.of(
                        StringUtf8Coder.of(),
                        SerializableCoder.of(EventSubjectActivity.class)));
        /**
         * Write event statistics on GCP bucket in AVRO format
         * New AVRO file for grouped statistics by each city
         */
        eventSubjectActivity.apply("Write results to GCP bucket in AVRO", FileIO.<String, KV<String, EventSubjectActivity>>writeDynamic()
                .withDestinationCoder(StringDelegateCoder.of(String.class))
                .by(KV::getKey)
                .via(Contextful.fn((SerializableFunction<KV<String, EventSubjectActivity>, EventSubjectActivity>) KV::getValue),
                        AvroIO.sink(EventSubjectActivity.class))
                .to(options.getOutput())
                .withNaming((SerializableFunction<String, FileIO.Write.FileNaming>) key -> FileIO.Write.defaultNaming(key, ".avro"))
        );

        pipeline.run().waitUntilFinish();
    }

    public static <T> ArrayList<T> getArrayListFromStream(Stream<T> stream) {
        return stream.collect(Collectors.toCollection(ArrayList::new));
    }

    private static class GenerateKVCityEventFn extends DoFn<Event, KV<String, Event>> {

        @ProcessElement
        public void processElement(@Element Event element, OutputReceiver<KV<String, Event>> receiver) {
            KV<String, Event> pair = KV.of(element.getCity(), element);
            receiver.output(pair);
        }
    }

    private static class DivideEventFn extends DoFn<KV<String, Iterable<Event>>, KV<String, List<KV<Subject, EventInfo>>>> {

        @ProcessElement
        public void processElement(@Element KV<String, Iterable<Event>> element, OutputReceiver<KV<String,
                List<KV<Subject, EventInfo>>>> receiver) {
            Iterable<Event> value = element.getValue();
            Stream<Event> eventStream = StreamSupport.stream(Objects.requireNonNull(value).spliterator(), false);

            List<KV<Subject, EventInfo>> kvList = eventStream.map((event) -> KV.of(event.getEventSubject(),
                    new EventInfo(event.getId(), event.getUserId(), event.getCity(), event.getEventType(),
                            event.getTimestamp()))).collect(Collectors.toList());

            KV<String, List<KV<Subject, EventInfo>>> pair = KV.of(element.getKey(), kvList);
            receiver.output(pair);
        }
    }

    private static class GetListOfPairsFn extends DoFn<KV<String, List<KV<Subject, EventInfo>>>, List<KV<Subject, EventInfo>>> {

        @ProcessElement
        public void processElement(@Element KV<String, List<KV<Subject, EventInfo>>> element,
                                   OutputReceiver<List<KV<Subject, EventInfo>>> receiver) {
            List<KV<Subject, EventInfo>> kvList = element.getValue();
            receiver.output(kvList);
        }
    }

    private static class GetSubjectEventPairsFn extends DoFn<List<KV<Subject, EventInfo>>, KV<Subject, EventInfo>> {

        @ProcessElement
        public void processElement(@Element List<KV<Subject, EventInfo>> element,
                                   OutputReceiver<KV<Subject, EventInfo>> receiver) {
            for (KV<Subject, EventInfo> subjectEventInfoKV : element) {
                receiver.output(subjectEventInfoKV);
            }
        }
    }

    private static class IterableToListFn extends DoFn<KV<Subject, Iterable<EventInfo>>, KV<Subject, List<EventInfo>>> {

        @ProcessElement
        public void processElement(@Element KV<Subject, Iterable<EventInfo>> element,
                                   OutputReceiver<KV<Subject, List<EventInfo>>> receiver) {

            Iterable<EventInfo> value = element.getValue();
            Stream<EventInfo> eventStream = StreamSupport.stream(Objects.requireNonNull(value).spliterator(), false);
            List<EventInfo> eventInfoList = getArrayListFromStream(eventStream);
            KV<Subject, List<EventInfo>> pair = KV.of(element.getKey(), eventInfoList);
            receiver.output(pair);
        }

    }

    private static class KVToListFn extends DoFn<KV<Subject, List<EventInfo>>, KV<String, KV<Subject, List<EventInfo>>>> {

        @ProcessElement
        public void processElement(@Element KV<Subject, List<EventInfo>> element,
                                   OutputReceiver<KV<String, KV<Subject, List<EventInfo>>>> receiver) {

            List<EventInfo> value = element.getValue();
            KV<String, KV<Subject, List<EventInfo>>> pair = KV.of(Objects.requireNonNull(value).stream()
                    .map(EventInfo::getCity).findAny().get(), element);
            receiver.output(pair);
        }
    }

    private static class IterableEventToListFn extends DoFn<KV<String, Iterable<KV<Subject, List<EventInfo>>>>,
            KV<String, List<KV<Subject, List<EventInfo>>>>> {

        @ProcessElement
        public void processElement(@Element KV<String, Iterable<KV<Subject, List<EventInfo>>>> element,
                                   OutputReceiver<KV<String, List<KV<Subject, List<EventInfo>>>>> receiver) {

            Iterable<KV<Subject, List<EventInfo>>> value = element.getValue();
            Stream<KV<Subject, List<EventInfo>>> kvStream = StreamSupport.stream(Objects.requireNonNull(value).spliterator(), false);
            List<KV<Subject, List<EventInfo>>> kvList = getArrayListFromStream(kvStream);
            KV<String, List<KV<Subject, List<EventInfo>>>> pair = KV.of(element.getKey(), kvList);
            receiver.output(pair);
        }
    }

    private static class EventInfoToActivityTransformFn extends DoFn<KV<String, List<KV<Subject, List<EventInfo>>>>
            , KV<String, Map<Subject, List<Activity>>>> {

        @ProcessElement
        public void processElement(@Element KV<String, List<KV<Subject, List<EventInfo>>>> element,
                                   OutputReceiver<KV<String, Map<Subject, List<Activity>>>> receiver) {

            List<KV<Subject, List<EventInfo>>> value = element.getValue();
            Map<Subject, List<EventInfo>> subjectListMap = new HashMap<>();

            for (KV<Subject, List<EventInfo>> kv : Objects.requireNonNull(value)) {
                subjectListMap.put(kv.getKey(), kv.getValue());
            }

            Map<Subject, List<Activity>> eventStatisticsMap = new HashMap<>();

            for (Map.Entry<Subject, List<EventInfo>> eventInfo : subjectListMap.entrySet()) {
                eventStatisticsMap.put(eventInfo.getKey(), generateActivities(eventInfo.getValue()));
            }

            KV<String, Map<Subject, List<Activity>>> pair = KV.of(element.getKey(), eventStatisticsMap);
            receiver.output(pair);
        }

        public static List<Activity> generateActivities(List<EventInfo> eventInfoList) {
            int weekCount = 7;
            int monthCount = 30;
            Map<String, List<EventInfo>> subjectTypeMap = new HashMap<>();

            for (EventInfo eventInfo : eventInfoList) {
                String eventType = eventInfo.getEventType();
                List<EventInfo> eventList = eventInfoList.stream().filter(s -> s.getEventType().equals(eventType)).collect(Collectors.toList());
                subjectTypeMap.put(eventType, eventList);
            }
            List<Activity> activities = new ArrayList<>();
            for (Map.Entry<String, List<EventInfo>> entry : subjectTypeMap.entrySet()) {
                Activity activity = new Activity();
                activity.setType(entry.getKey());
                activity.setPast7daysCount(pastDaysCount(entry.getValue(), weekCount, false));
                activity.setPast7daysUniqueCount(pastDaysCount(entry.getValue(), weekCount, true));
                activity.setPast30daysCount(pastDaysCount(entry.getValue(), monthCount, false));
                activity.setPast30daysUniqueCount(pastDaysCount(entry.getValue(), monthCount, true));
                activities.add(activity);
            }
            return activities;
        }

        public static int pastDaysCount(List<EventInfo> eventInfoList, int amountOfDays, boolean isUniqueUser) {
            LocalDate localDate = LocalDate.now();
            int count = 0;
            long epochDay = localDate.toEpochDay();
            for (EventInfo eventInfo : eventInfoList) {
                long difference = epochDay - eventInfo.getTimestamp();
                if (difference <= amountOfDays) {
                    if (isUniqueUser) {
                        count = (int) eventInfoList.stream().map(EventInfo::getUserId).distinct().count();
                        return count;
                    }
                    count++;
                }
            }
            return count;
        }
    }

    private static class MapToEventSubjectActivityFn extends DoFn<KV<String, Map<Subject, List<Activity>>>,
            KV<String, EventSubjectActivity>> {

        @ProcessElement
        public void processElement(@Element KV<String, Map<Subject, List<Activity>>> element,
                                   OutputReceiver<KV<String, EventSubjectActivity>> receiver) {

            EventSubjectActivity eventSubjectActivity = new EventSubjectActivity();
            eventSubjectActivity.setSubjects(element.getValue());
            KV<String, EventSubjectActivity> pair = KV.of(element.getKey(), eventSubjectActivity);
            receiver.output(pair);
        }
    }
}
