# kryo-kafka [WIP]

User-friendly library implementing [Kryo](https://github.com/EsotericSoftware/kryo) serializers for
[Apache Kafka](https://github.com/apache/kafka).

### TODO

 - [x] Add integration test using testcontainers
 - [ ] Figure out what's wrong with PooledKryoConcurrencyStrategy performance (github issue link to come)
 - [ ] Add github action to run test suite on PRs
 - [ ] Add github action to preform maven releases
 - [ ] Investigate custom serializer configurations

# Usage
Producer:
```java
new KafkaProducer<>(Map.of(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KryoSerializer.class,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KryoSerializer.class,
        KRYO_SERIALIZATION_REGISTERED_CLASSES.toString(), new Class<?>[]{CustomObject.class},
//      KRYO_SERIALIZATION_REGISTERED_CLASSES_REQUIRED.toString(), false,
        KRYO_SERIALIZATION_CONCURRENCY_STRATEGY.toString(), ThreadLocalKryoConcurrencyStrategy.class
));
```
Consumer:
```java
new KafkaConsumer<>(Map.of(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
        ConsumerConfig.GROUP_ID_CONFIG, "your-group-id",
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KryoDeserializer.class,
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KryoDeserializer.class,
        KRYO_SERIALIZATION_REGISTERED_CLASSES.toString(), new Class<?>[]{CustomObject.class}
));
```
Streams Consumer:
```java
Properties properties = new Properties();
properties.putAll(Map.of(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers(),
        StreamsConfig.APPLICATION_ID_CONFIG, topic,
        StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, KryoSerde.class,
        StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, KryoSerde.class,
        KRYO_SERIALIZATION_CONCURRENCY_STRATEGY.toString(), clazz));
...
KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), properties);
```

## Concurrency Strategies

Instances of `com.esotericsoftware.kryo.Kryo`, `com.esotericsoftware.kryo.io.Input` and
`com.esotericsoftware.kryo.io.Output` are not thread safe, as a result we need to handle highly concurrent usages of the
serializers when configuring it with your Kafka producers and consumers.

To configure this you can add a
`KRYO_SERIALIZATION_CONCURRENCY_STRATEGY` to your config which can be one of
[these strategies](src/main/java/github/stephenflavin/kryo/kafka/strategy).

 - `SynchronizedKryoConcurrencyStrategy`
   - This strategy creates a single instance of Kryo on initialization and lazy loads Output/Input, all interactions 
   with these are then done via
   [`synchronized`](https://docs.oracle.com/javase/tutorial/essential/concurrency/syncmeth.html) methods to make them
   thread safe.
 - `ThreadLocalKryoConcurrencyStrategy`
   - This strategy makes use of
   [`ThreadLocal#withInitial`](https://docs.oracle.com/javase/8/docs/api/java/lang/ThreadLocal.html#withInitial-java.util.function.Supplier-)
   for instances of Kryo, Input and Output; this ensures that each thread has its own instance to use.
 - `PooledKryoConcurrencyStrategy`
   - This strategy makes use of `com.esotericsoftware.kryo.util.Pool` to create a pool of Kryo, Input and Output
   instances which are shared with all threads.
   - There is additional optional configuration for this strategy using `KRYO_SERIALIZATION_CONCURRENCY_POOL_SIZE` which
   controls the maximum number of parallel serialization processes.

For Additional information on Kryo thread safety and pooling see
[kryo#thread-safety](https://github.com/EsotericSoftware/kryo#thread-safety).
 
## Configuration

All configuration options are stored in an enum
[KryoSerializationConfig.java](src/main/java/github/stephenflavin/kryo/kafka/config/KryoSerializationConfig.java),
values have a string representation of their name (used in kafka config), a `Predicate` to demonstrate what things they
accept and a *default* value.
