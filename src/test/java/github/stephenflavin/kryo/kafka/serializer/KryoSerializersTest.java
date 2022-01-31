package github.stephenflavin.kryo.kafka.serializer;

import static github.stephenflavin.kryo.kafka.config.KryoSerializationConfig.KRYO_SERIALIZATION_CONCURRENCY_STRATEGY;
import static github.stephenflavin.kryo.kafka.config.KryoSerializationConfig.KRYO_SERIALIZATION_REGISTERED_CLASSES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
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
        ComplexObject expectedOutput = new ComplexObject("moo", 1, false, likes, entities);

        Map<String, ?> config = Map.of(KRYO_SERIALIZATION_CONCURRENCY_STRATEGY.toString(), clazz,
                KRYO_SERIALIZATION_REGISTERED_CLASSES.toString(), new Class<?>[]{ComplexObject.class,
                        likes.getClass(),
                        entities.getClass(),
                        foobar.getClass(),
                        Map[].class});

        KryoSerializer kryoSerializer = new KryoSerializer();
        kryoSerializer.configure(config, true);
        byte[] actualSerializedValue = kryoSerializer.serialize(null, expectedOutput);

        KryoDeserializer kryoDeserializer = new KryoDeserializer();
        kryoDeserializer.configure(config, true);
        ComplexObject actualDeserializedValue = (ComplexObject) kryoDeserializer
                .deserialize(null, actualSerializedValue);

        assertEquals(expectedOutput, actualDeserializedValue);
    }

    @ParameterizedTest
    @ValueSource(classes = {SynchronizedKryoConcurrencyStrategy.class,
            PooledKryoConcurrencyStrategy.class,
            ThreadLocalKryoConcurrencyStrategy.class})
    public void parallel_serialize_deserialize(Class<?> clazz) {
        List<String> expectedOutput = new ArrayList<>();
        for (int i = 0; i < 1000000; i++) {
            expectedOutput.add(UUID.randomUUID().toString());
        }

        Map<String, ?> config = Map.of(KRYO_SERIALIZATION_CONCURRENCY_STRATEGY.toString(), clazz);

        KryoSerializer kryoSerializer = new KryoSerializer();
        kryoSerializer.configure(config, true);

        KryoDeserializer kryoDeserializer = new KryoDeserializer();
        kryoDeserializer.configure(config, true);

        Set<String> output = expectedOutput.stream()
                .parallel()
                .map(s -> kryoSerializer.serialize(null, s))
                .map(b -> kryoDeserializer.deserialize(null, b))
                .map(String.class::cast)
                .collect(Collectors.toSet());

        assertTrue(output.containsAll(expectedOutput));
    }

    public record ComplexObject(String string, int integer, boolean bool, Map<?, ?>... primitiveArrMaps) {
        @Override
        public boolean equals(Object otherObj) {
            if (!(otherObj instanceof ComplexObject)) {
                return false;
            }
            ComplexObject other = (ComplexObject) otherObj;
            return this.string.equals(other.string) &&
                    this.integer == other.integer &&
                    this.bool == other.bool &&
                    Arrays.equals(this.primitiveArrMaps, other.primitiveArrMaps);
        }
    }

}