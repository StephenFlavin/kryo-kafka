package github.stephenflavin.kryo.kafka;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import github.stephenflavin.kryo.kafka.config.KryoSerializationConfig;

public class Utility {

    public static Map<KryoSerializationConfig, ?> extractAndValidateKryoConfigs(Map<String, ?> configs) {
        Map<String, KryoSerializationConfig> kryoConfigNames = Arrays.stream(KryoSerializationConfig.values())
                .collect(Collectors.toMap(KryoSerializationConfig::getName, Function.identity()));
        Map<KryoSerializationConfig, ?> configMap = configs.entrySet().stream()
                .filter(entry -> kryoConfigNames.containsKey(entry.getKey()))
                .collect(Collectors.toMap(k -> kryoConfigNames.get(k.getKey()), Map.Entry::getValue));
        validateEntries(configMap);
        return configMap;
    }

    private static void validateEntries(Map<KryoSerializationConfig, ?> configs) {
        for (Map.Entry<KryoSerializationConfig, ?> entry : configs.entrySet()) {
            KryoSerializationConfig key = entry.getKey();
            Object value = entry.getValue();
            if (!key.test(value)) {
                throw new IllegalArgumentException("Invalid value provided for " +
                        key +
                        " (" +
                        value.getClass().getSimpleName() +
                        "). See github.stephenflavin.kryo.kafka.config.KryoSerializationConfig for more information");
            }
        }
    }
}
