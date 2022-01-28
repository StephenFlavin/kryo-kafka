package github.stephenflavin.kryo.kafka.strategy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import github.stephenflavin.kryo.kafka.config.KryoSerializationConfig;

public class SynchronizedKryoConcurrencyStrategy extends KryoConcurrencyStrategy {

    private final Kryo kryo;

    private Output output;
    private Input input;

    public SynchronizedKryoConcurrencyStrategy(Map<KryoSerializationConfig, ?> configMap) {
        super(configMap);
        this.kryo = generateKryoInstance();
    }

    public byte[] serialize(Object o) {
        if (o == null) {
            return EMPTY_BYTE_ARRAY;
        }
        if (output == null) {
            output = generateOutputInstance();
        }
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            output.setOutputStream(stream);
            kryo.writeClassAndObject(output, o);
            output.flush();
            return stream.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public Object deserialize(byte[] data) {
        if (data == null || data.length == 0) {
            return null;
        }
        if (input == null) {
            input = generateInputInstance();
        }
        input.setBuffer(data);
        return kryo.readClassAndObject(input);
    }
}
