package myapps;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class HotelPageViewAgg {


    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);


        final StreamsBuilder builder = new StreamsBuilder();


        final KStream<String, String> source = builder.stream("events-string");
        source
                .groupBy((key, value) -> {
                    List<String> values = Arrays.asList(value.split(","));
                    Date date = Date.from(Instant.ofEpochMilli(Long.valueOf(values.get(0))));
                    Integer hotelId = Integer.valueOf(values.get(1));
                    String eventType = values.get(2);

                    String newKey = String.format("%d%s", hotelId, eventType);
                    return newKey;
                })
                .windowedBy(SessionWindows.with(TimeUnit.MINUTES.toMillis(5)))
                .count()
                .toStream()
                .print(Printed.toSysOut());


        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
