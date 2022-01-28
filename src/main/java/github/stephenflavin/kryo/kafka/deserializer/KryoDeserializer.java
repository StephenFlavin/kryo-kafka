package github.stephenflavin.kryo.kafka.deserializer;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import github.stephenflavin.kryo.kafka.Utility;
import github.stephenflavin.kryo.kafka.factory.KryoConcurrencyStrategyFactory;
import github.stephenflavin.kryo.kafka.strategy.KryoConcurrencyStrategy;

public class KryoDeserializer implements Deserializer<Object> {

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
    public Object deserialize(String topic, byte[] data) {
        return kryoConcurrencyStrategy.deserialize(data);
    }
}
