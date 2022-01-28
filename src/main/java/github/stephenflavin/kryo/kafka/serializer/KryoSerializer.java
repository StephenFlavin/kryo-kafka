package github.stephenflavin.kryo.kafka.serializer;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;

import github.stephenflavin.kryo.kafka.Utility;
import github.stephenflavin.kryo.kafka.factory.KryoConcurrencyStrategyFactory;
import github.stephenflavin.kryo.kafka.strategy.KryoConcurrencyStrategy;

public class KryoSerializer implements Serializer<Object> {

    private KryoConcurrencyStrategy kryoConcurrencyStrategy;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        try {
            kryoConcurrencyStrategy = KryoConcurrencyStrategyFactory.get(Utility.extractAndValidateKryoConfigs(configs));
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException | InstantiationException e) {
            e.printStackTrace();
        }
    }

    @Override
    public byte[] serialize(String topic, Object data) {
        return kryoConcurrencyStrategy.serialize(data);
    }
}
