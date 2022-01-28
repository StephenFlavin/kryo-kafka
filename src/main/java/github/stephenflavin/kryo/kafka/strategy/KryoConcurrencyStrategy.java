package github.stephenflavin.kryo.kafka.strategy;

import static github.stephenflavin.kryo.kafka.config.KryoSerializationConfig.*;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import github.stephenflavin.kryo.kafka.config.KryoSerializationConfig;

public abstract class KryoConcurrencyStrategy {

    protected static final byte[] EMPTY_BYTE_ARRAY = new byte[]{};

    protected final Map<KryoSerializationConfig, ?> configs;

    public KryoConcurrencyStrategy(Map<KryoSerializationConfig, ?> configs) {
        this.configs = configs;
    }

    public abstract byte[] serialize(Object o);

    public abstract Object deserialize(byte[] data);

    protected Output generateOutputInstance() {
        Integer bufferSize = Optional.ofNullable(configs.get(KRYO_SERIALIZATION_OUTPUT_BUFFER_SIZE))
                .map(Integer.class::cast)
                .orElse((Integer) KRYO_SERIALIZATION_OUTPUT_BUFFER_SIZE.getDefaultValue());
        Integer bufferMaxSize = Optional.ofNullable(configs.get(KRYO_SERIALIZATION_OUTPUT_BUFFER_MAX_SIZE))
                .map(Integer.class::cast)
                .orElse((Integer) KRYO_SERIALIZATION_OUTPUT_BUFFER_MAX_SIZE.getDefaultValue());
        return new Output(bufferSize, bufferMaxSize);
    }

    protected Input generateInputInstance() {
        Integer bufferSize = Optional.ofNullable(configs.get(KRYO_SERIALIZATION_INPUT_BUFFER_SIZE))
                .map(Integer.class::cast)
                .orElse((Integer) KRYO_SERIALIZATION_INPUT_BUFFER_SIZE.getDefaultValue());
        return new Input(bufferSize);
    }

    protected Kryo generateKryoInstance() {
        Kryo kryo = new Kryo();

        if (configs.containsKey(KRYO_SERIALIZATION_REGISTERED_CLASSES_REQUIRED)) {
            kryo.setRegistrationRequired(false);
        } else {
            Optional.ofNullable(configs.get(KRYO_SERIALIZATION_REGISTERED_CLASSES))
                    .map(Class[].class::cast)
                    .map(Arrays::stream)
                    .ifPresent(classStream -> classStream.forEach(kryo::register));
        }

        return kryo;
    }

}
