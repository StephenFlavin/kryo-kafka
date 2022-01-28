package github.stephenflavin.kryo.kafka.streams.serde;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import github.stephenflavin.kryo.kafka.deserializer.KryoDeserializer;
import github.stephenflavin.kryo.kafka.serializer.KryoSerializer;

public class KryoSerde implements Serde<Object> {

    private Serdes.WrapperSerde<Object> serde;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        KryoSerializer kryoSerializer = new KryoSerializer();
        kryoSerializer.configure(configs, isKey);
        KryoDeserializer kryoDeserializer = new KryoDeserializer();
        kryoDeserializer.configure(configs, isKey);
        this.serde = new Serdes.WrapperSerde<>(kryoSerializer, kryoDeserializer);
    }

    @Override
    public Serializer<Object> serializer() {
        return serde.serializer();
    }

    @Override
    public Deserializer<Object> deserializer() {
        return serde.deserializer();
    }
}
