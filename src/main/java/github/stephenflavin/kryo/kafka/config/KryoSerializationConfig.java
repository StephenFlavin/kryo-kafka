package github.stephenflavin.kryo.kafka.config;

import java.util.function.Predicate;

import github.stephenflavin.kryo.kafka.strategy.KryoConcurrencyStrategy;
import github.stephenflavin.kryo.kafka.strategy.SynchronizedKryoConcurrencyStrategy;

public enum KryoSerializationConfig {
    KRYO_SERIALIZATION_CONCURRENCY_STRATEGY("kryo.serialization.concurrency.strategy",
            o -> o instanceof Class<?> && KryoConcurrencyStrategy.class.isAssignableFrom((Class<?>)o),
            SynchronizedKryoConcurrencyStrategy.class),
    KRYO_SERIALIZATION_REGISTERED_CLASSES("kryo.serialization.registered.classes",
            o -> true,
            null),
    KRYO_SERIALIZATION_REGISTERED_CLASSES_REQUIRED("kryo.serialization.registered.classes.required",
            o -> o instanceof Boolean,
            true),
    KRYO_SERIALIZATION_OUTPUT_BUFFER_SIZE("kryo.serialization.output.buffer.size",
            o -> o instanceof Integer,
            1024),
    KRYO_SERIALIZATION_OUTPUT_BUFFER_MAX_SIZE("kryo.serialization.output.buffer.max.size",
            o -> o instanceof Integer,
            -1),
    KRYO_SERIALIZATION_INPUT_BUFFER_SIZE("kryo.serialization.input.buffer.size",
            o -> o instanceof Integer,
            4096),
    KRYO_SERIALIZATION_CONCURRENCY_POOL_SIZE("kryo.serialization.concurrency.pool.size",
            o -> o instanceof Integer,
            8);

    private final String name;
    private final Predicate<Object> expectedValuePredicate;
    private final Object defaultValue;

    KryoSerializationConfig(String name, Predicate<Object> expectedValuePredicate, Object defaultValue) {
        this.name = name;
        this.expectedValuePredicate = expectedValuePredicate;
        this.defaultValue = defaultValue;
    }

    public String getName() {
        return name;
    }

    public boolean test(Object o) {
        return expectedValuePredicate.test(o);
    }

    public Object getDefaultValue() {
        return defaultValue;
    }

    @Override
    public String toString() {
        return getName();
    }
}
