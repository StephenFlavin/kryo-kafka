package github.stephenflavin.kryo.kafka.serializer;

import static github.stephenflavin.kryo.kafka.config.KryoSerializationConfig.KRYO_SERIALIZATION_CONCURRENCY_STRATEGY;
import static github.stephenflavin.kryo.kafka.config.KryoSerializationConfig.KRYO_SERIALIZATION_REGISTERED_CLASSES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.utility.DockerImageName;

import github.stephenflavin.kryo.kafka.deserializer.KryoDeserializer;
import github.stephenflavin.kryo.kafka.strategy.PooledKryoConcurrencyStrategy;
import github.stephenflavin.kryo.kafka.strategy.SynchronizedKryoConcurrencyStrategy;
import github.stephenflavin.kryo.kafka.strategy.ThreadLocalKryoConcurrencyStrategy;
import github.stephenflavin.kryo.kafka.streams.serde.KryoSerde;

@Order(Integer.MAX_VALUE)
public class KafkaIntegrationTest {

    private static final KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName
            .parse("confluentinc/cp-kafka:6.2.1"));
    private static AdminClient adminClient;

    static {
        kafkaContainer.start();

        adminClient = AdminClient.create(Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafkaContainer.getBootstrapServers()));

    }

    @ParameterizedTest
    @ValueSource(classes = {SynchronizedKryoConcurrencyStrategy.class,
            PooledKryoConcurrencyStrategy.class,
            ThreadLocalKryoConcurrencyStrategy.class})
    public void simple_serialize_deserialize(Class<?> clazz) throws ExecutionException, InterruptedException {
        String topic = createTopic();

        String key = "Key";
        String value = "Value";

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, topic,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KryoDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KryoDeserializer.class,
                KRYO_SERIALIZATION_CONCURRENCY_STRATEGY.toString(), clazz,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
        ));
        consumer.subscribe(Collections.singleton(topic));

        KafkaProducer<String, String> producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KryoSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KryoSerializer.class,
                KRYO_SERIALIZATION_CONCURRENCY_STRATEGY.toString(), clazz
        ));

        producer.send(new ProducerRecord<>(topic, key, value)).get();

        ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMinutes(1));

        assertEquals(consumerRecords.count(), 1);
        ConsumerRecord<String, String> record = consumerRecords.iterator().next();
        assertEquals(record.key(), key);
        assertEquals(record.value(), value);
    }

    @ParameterizedTest
    @ValueSource(classes = {SynchronizedKryoConcurrencyStrategy.class,
            PooledKryoConcurrencyStrategy.class,
            ThreadLocalKryoConcurrencyStrategy.class})
    public void custom_serialize_deserialize(Class<?> clazz) throws ExecutionException, InterruptedException {
        String topic = createTopic();

        List<String> foobar = List.of("foo", "bar");
        Map<List<String>, Long> likes = Map.of(foobar, Long.MAX_VALUE);
        Map<String, Double> entities = Map.of("baz", 0.8, "zoo", 0.5);
        KryoSerializersTest.ComplexObject complexObject = new KryoSerializersTest.ComplexObject("moo",
                1,
                false,
                likes,
                entities);

        KafkaConsumer<Object, Object> consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, topic,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KryoDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KryoDeserializer.class,
                KRYO_SERIALIZATION_CONCURRENCY_STRATEGY.toString(), clazz,
                KRYO_SERIALIZATION_REGISTERED_CLASSES.toString(), new Class<?>[]{KryoSerializersTest.ComplexObject.class,
                        likes.getClass(),
                        entities.getClass(),
                        foobar.getClass(),
                        Map[].class},
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
        ));
        consumer.subscribe(Collections.singleton(topic));

        KafkaProducer<Object, Object> producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KryoSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KryoSerializer.class,
                KRYO_SERIALIZATION_CONCURRENCY_STRATEGY.toString(), clazz,
                KRYO_SERIALIZATION_REGISTERED_CLASSES.toString(), new Class<?>[]{KryoSerializersTest.ComplexObject.class,
                        likes.getClass(),
                        entities.getClass(),
                        foobar.getClass(),
                        Map[].class}
        ));

        producer.send(new ProducerRecord<>(topic, complexObject, complexObject)).get();

        ConsumerRecords<Object, Object> consumerRecords = consumer.poll(Duration.ofMinutes(1));

        assertEquals(consumerRecords.count(), 1);
        ConsumerRecord<Object, Object> record = consumerRecords.iterator().next();
        assertEquals(record.key(), complexObject);
        assertEquals(record.value(), complexObject);
    }

    @ParameterizedTest
    @ValueSource(classes = {SynchronizedKryoConcurrencyStrategy.class,
            PooledKryoConcurrencyStrategy.class,
            ThreadLocalKryoConcurrencyStrategy.class})
    public void kafka_streams_simple_serialize_deserialize(Class<?> clazz) throws ExecutionException, InterruptedException {
        String topic = createTopic();

        String key = "Key";
        String value = "Value";

        Properties properties = new Properties();
        properties.putAll(Map.of(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers(),
                StreamsConfig.APPLICATION_ID_CONFIG, topic,
                StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, KryoSerde.class,
                StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, KryoSerde.class,
                KRYO_SERIALIZATION_CONCURRENCY_STRATEGY.toString(), clazz));

        AtomicInteger counsumedMessageCounter = new AtomicInteger();
        StreamsBuilder builder = new StreamsBuilder();
        KTable<Object, Object> consumed = builder.stream(topic)
                .peek((k, v) -> counsumedMessageCounter.getAndIncrement())
                .toTable(Materialized.as("consumed"));

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);
        streams.start();

        Awaitility.await()
                .pollInterval(Duration.ofMillis(10))
                .atMost(Duration.ofMinutes(1))
                .until(streams::state, KafkaStreams.State.RUNNING::equals);

        KafkaProducer<String, String> producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KryoSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KryoSerializer.class,
                KRYO_SERIALIZATION_CONCURRENCY_STRATEGY.toString(), clazz
        ));

        producer.send(new ProducerRecord<>(topic, key, value)).get();

        ReadOnlyKeyValueStore<Object, Object> consumedTable = streams.store(
                StoreQueryParameters.fromNameAndType("consumed", QueryableStoreTypes.keyValueStore())
        );

        Awaitility.await()
                .pollInterval(Duration.ofMillis(10))
                .atMost(Duration.ofMinutes(1))
                .until(consumedTable::all, Iterator::hasNext);

        KeyValueIterator<Object, Object> keyValues = consumedTable.all();
        assertTrue(keyValues.hasNext());
        KeyValue<Object, Object> firstKeyValue = keyValues.next();
        assertEquals(firstKeyValue.key, key);
        assertEquals(firstKeyValue.value, value);
    }

    private String createTopic() {
        String topic = UUID.randomUUID().toString();
        adminClient.createTopics(Collections.singleton(new NewTopic(topic, 1, (short) 1)));
        return topic;
    }

}
