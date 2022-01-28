package github.stephenflavin.kryo.kafka.factory;

import static github.stephenflavin.kryo.kafka.config.KryoSerializationConfig.KRYO_SERIALIZATION_CONCURRENCY_STRATEGY;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Optional;

import github.stephenflavin.kryo.kafka.config.KryoSerializationConfig;
import github.stephenflavin.kryo.kafka.strategy.KryoConcurrencyStrategy;

public class KryoConcurrencyStrategyFactory {

    public static KryoConcurrencyStrategy get(Map<KryoSerializationConfig, ?> configMap) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        Class<KryoConcurrencyStrategy> clazz = Optional.ofNullable(
                        configMap.get(KRYO_SERIALIZATION_CONCURRENCY_STRATEGY))
                .map(obj -> (Class<KryoConcurrencyStrategy>) obj)
                .orElse((Class<KryoConcurrencyStrategy>) KRYO_SERIALIZATION_CONCURRENCY_STRATEGY.getDefaultValue());

        return clazz.getConstructor(Map.class).newInstance(configMap);
    }

}
