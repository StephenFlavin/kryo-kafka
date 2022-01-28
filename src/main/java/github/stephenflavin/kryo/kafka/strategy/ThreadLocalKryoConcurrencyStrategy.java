package github.stephenflavin.kryo.kafka.strategy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import github.stephenflavin.kryo.kafka.config.KryoSerializationConfig;

public class ThreadLocalKryoConcurrencyStrategy extends KryoConcurrencyStrategy {

    private final ThreadLocal<Kryo> kryoThreadLocal;
    private final ThreadLocal<Output> outputThreadLocal;
    private final ThreadLocal<Input> inputThreadLocal;

    public ThreadLocalKryoConcurrencyStrategy(Map<KryoSerializationConfig, ?> configMap) {
        super(configMap);
        this.kryoThreadLocal = ThreadLocal.withInitial(this::generateKryoInstance);
        this.outputThreadLocal = ThreadLocal.withInitial(this::generateOutputInstance);
        this.inputThreadLocal = ThreadLocal.withInitial(this::generateInputInstance);
    }

    public byte[] serialize(Object o) {
        if (o == null) {
            return EMPTY_BYTE_ARRAY;
        }
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            Output output = outputThreadLocal.get();
            output.setOutputStream(stream);
            kryoThreadLocal.get().writeClassAndObject(output, o);
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
        Input input = inputThreadLocal.get();
        input.setBuffer(data);
        return kryoThreadLocal.get().readClassAndObject(input);
    }
}
