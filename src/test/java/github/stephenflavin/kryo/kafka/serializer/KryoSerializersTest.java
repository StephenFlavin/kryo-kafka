package github.stephenflavin.kryo.kafka.serializer;

import static github.stephenflavin.kryo.kafka.config.KryoSerializationConfig.KRYO_SERIALIZATION_CONCURRENCY_STRATEGY;
import static github.stephenflavin.kryo.kafka.config.KryoSerializationConfig.KRYO_SERIALIZATION_REGISTERED_CLASSES;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import github.stephenflavin.kryo.kafka.deserializer.KryoDeserializer;
import github.stephenflavin.kryo.kafka.strategy.PooledKryoConcurrencyStrategy;
import github.stephenflavin.kryo.kafka.strategy.SynchronizedKryoConcurrencyStrategy;
import github.stephenflavin.kryo.kafka.strategy.ThreadLocalKryoConcurrencyStrategy;

class KryoSerializersTest {

    @ParameterizedTest
    @ValueSource(classes = {SynchronizedKryoConcurrencyStrategy.class,
            PooledKryoConcurrencyStrategy.class,
            ThreadLocalKryoConcurrencyStrategy.class})
    public void simple_serialize_deserialize(Class<?> clazz) {
        String expectedOutput = "Serialize Me!";

        Map<String, ?> config = Map.of(KRYO_SERIALIZATION_CONCURRENCY_STRATEGY.toString(), clazz);

        KryoSerializer kryoSerializer = new KryoSerializer();
        kryoSerializer.configure(config, true);
        byte[] actualSerializedValue = kryoSerializer.serialize(null, expectedOutput);

        KryoDeserializer kryoDeserializer = new KryoDeserializer();
        kryoDeserializer.configure(config, true);
        Object actualDeserializedValue = kryoDeserializer.deserialize(null, actualSerializedValue);

        assertEquals(expectedOutput, actualDeserializedValue);
    }

    @ParameterizedTest
    @ValueSource(classes = {SynchronizedKryoConcurrencyStrategy.class,
            PooledKryoConcurrencyStrategy.class,
            ThreadLocalKryoConcurrencyStrategy.class})
    public void custom_serialize_deserialize(Class<?> clazz) {
        List<String> foobar = List.of("foo", "bar");
        Map<List<String>, Long> likes = Map.of(foobar, Long.MAX_VALUE);
        Map<String, Double> entities = Map.of("baz", 0.8, "zoo", 0.5);
        CustomObject expectedOutput = new CustomObject("moo", 1, false, likes, entities);

        Map<String, ?> config = Map.of(KRYO_SERIALIZATION_CONCURRENCY_STRATEGY.toString(), clazz,
                KRYO_SERIALIZATION_REGISTERED_CLASSES.toString(), new Class<?>[]{CustomObject.class,
                        likes.getClass(),
                        entities.getClass(),
                        foobar.getClass(),
                        Map[].class});

        KryoSerializer kryoSerializer = new KryoSerializer();
        kryoSerializer.configure(config, true);
        byte[] actualSerializedValue = kryoSerializer.serialize(null, expectedOutput);

        KryoDeserializer kryoDeserializer = new KryoDeserializer();
        kryoDeserializer.configure(config, true);
        CustomObject actualDeserializedValue = (CustomObject) kryoDeserializer
                .deserialize(null, actualSerializedValue);

        assertEquals(expectedOutput.string, actualDeserializedValue.string);
        assertEquals(expectedOutput.integer, actualDeserializedValue.integer);
        assertEquals(expectedOutput.bool, actualDeserializedValue.bool);
        assertArrayEquals(expectedOutput.primitiveArrMaps, actualDeserializedValue.primitiveArrMaps);
    }

    static List<String> largeUUIDCollection = new ArrayList<>();

    static {
        for (int i = 0; i < 5000000; i++)
            largeUUIDCollection.add(UUID.randomUUID().toString());
    }

    //todo cleanup after raising issue about EsotericSoftware/kryo#pooling
    @ParameterizedTest
    @ValueSource(classes = {SynchronizedKryoConcurrencyStrategy.class,
            PooledKryoConcurrencyStrategy.class,
            ThreadLocalKryoConcurrencyStrategy.class})
    public void parallel_serialize_deserialize(Class<?> clazz) {
        Map<String, ?> config = Map.of(KRYO_SERIALIZATION_CONCURRENCY_STRATEGY.toString(), clazz);

        KryoSerializer kryoSerializer = new KryoSerializer();
        kryoSerializer.configure(config, true);

        KryoDeserializer kryoDeserializer = new KryoDeserializer();
        kryoDeserializer.configure(config, true);

        System.out.print(clazz.getSimpleName() + " -> ");
        long total = 0;
        long serializingTime = 0;
        long deserializingTime = 0;

        for (int i = 0; i < 10; i++) {
            Instant start = Instant.now();
            List<byte[]> serialised = largeUUIDCollection.stream()
                    .parallel()
                    .map(s -> kryoSerializer.serialize(null, s))
                    .toList();
            Instant finishedSerializing = Instant.now();
            serializingTime += Duration.between(start, finishedSerializing).toMillis();
            Set<String> output = serialised.stream()
                    .map(b -> kryoDeserializer.deserialize(null, b))
                    .parallel()
                    .map(String.class::cast)
                    .collect(Collectors.toSet());
            Instant roundTripTime = Instant.now();
            deserializingTime += Duration.between(finishedSerializing, roundTripTime).toMillis();
            total += Duration.between(start, roundTripTime).toMillis();
            assertTrue(output.containsAll(largeUUIDCollection));
        }

        long averageRoundTrip = total / 10;
        long averageSerializing = serializingTime / 10;
        long averageDeserializing = deserializingTime / 10;

        System.out.println("Average Time Spent Processing " +
                Duration.ofMillis(averageRoundTrip) +
                " ( serializing: " +
                Duration.ofMillis(averageSerializing) +
                ", deserializing: " +
                Duration.ofMillis(averageDeserializing) +
                " )");
    }

    public static record CustomObject(String string, int integer, boolean bool, Map<?, ?>... primitiveArrMaps) {
    }

}