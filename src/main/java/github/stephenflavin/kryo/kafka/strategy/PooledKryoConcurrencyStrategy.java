package github.stephenflavin.kryo.kafka.strategy;

import static github.stephenflavin.kryo.kafka.config.KryoSerializationConfig.KRYO_SERIALIZATION_CONCURRENCY_POOL_SIZE;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.util.Pool;

import github.stephenflavin.kryo.kafka.config.KryoSerializationConfig;

public class PooledKryoConcurrencyStrategy extends KryoConcurrencyStrategy {

    private final Pool<Kryo> kryoPool;
    private final Pool<Output> outputPool;
    private final Pool<Input> inputPool;

    public PooledKryoConcurrencyStrategy(Map<KryoSerializationConfig, ?> configMap) {
        super(configMap);
        Integer maxConcurrency = Optional.ofNullable(configMap.get(KRYO_SERIALIZATION_CONCURRENCY_POOL_SIZE))
                .map(Integer.class::cast)
                .orElse((Integer) KRYO_SERIALIZATION_CONCURRENCY_POOL_SIZE.getDefaultValue());
        this.kryoPool = new Pool<>(true, true, maxConcurrency) {
            protected Kryo create() {
                return generateKryoInstance();
            }
        };
        this.outputPool = new Pool<>(true, true, maxConcurrency) {
            protected Output create() {
                return generateOutputInstance();
            }
        };
        this.inputPool = new Pool<>(true, true, maxConcurrency) {
            protected Input create() {
                return generateInputInstance();
            }
        };
    }

    @Override
    public byte[] serialize(Object o) {
        if (o == null) {
            return EMPTY_BYTE_ARRAY;
        }
        Kryo kryo = null;
        Output output = null;
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            kryo = kryoPool.obtain();
            output = outputPool.obtain();
            output.setOutputStream(stream);
            kryo.writeClassAndObject(output, o);
            output.flush();
            return stream.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        } finally {
            if (kryo != null)
                kryoPool.free(kryo);
            if (output != null)
                outputPool.free(output);
        }
    }

    @Override
    public Object deserialize(byte[] data) {
        if (data == null || data.length == 0) {
            return null;
        }
        Kryo kryo = null;
        Input input = null;
        try {
            kryo = kryoPool.obtain();
            input = inputPool.obtain();
            input.setBuffer(data);
            return kryo.readClassAndObject(input);
        } finally {
            if (kryo != null)
                kryoPool.free(kryo);
            if (input != null)
                inputPool.free(input);
        }
    }
}
