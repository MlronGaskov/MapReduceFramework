package ru.nsu.mr;

import java.util.Comparator;

public class MapReduceJob<K1, V1, K2, V2> {
    private final Mapper<String, String, K1, V1> mapper;
    private final Reducer<K1, V1, K2, V2> reducer;
    private final PrecombineFunction<K1, V1, K2, V2> precombineFunction;
    private final Serializer<K1> serializerInterKey;
    private final Serializer<V1> serializerInterValue;
    private final Deserializer<K1> deserializerInterKey;
    private final Deserializer<V1> deserializerInterValue;
    private final Serializer<K2> serializerOutKey;
    private final Serializer<V2> serializerOutValue;
    private final Comparator<K1> comparator;
    private final KeyHasher<K1> hasher;

    public MapReduceJob(
            Mapper<String, String, K1, V1> mapper,
            Reducer<K1, V1, K2, V2> reducer, PrecombineFunction<K1, V1, K2, V2> precombineFunction,
            Serializer<K1> serializerInterKey,
            Serializer<V1> serializerInterValue,
            Deserializer<K1> deserializerInterKey,
            Deserializer<V1> deserializerInterValue,
            Serializer<K2> serializerOutKey,
            Serializer<V2> serializerOutValue,
            Comparator<K1> comparator,
            KeyHasher<K1> hasher) {
        this.mapper = mapper;
        this.reducer = reducer;
        this.precombineFunction = precombineFunction;
        this.serializerInterKey = serializerInterKey;
        this.serializerInterValue = serializerInterValue;
        this.deserializerInterKey = deserializerInterKey;
        this.deserializerInterValue = deserializerInterValue;
        this.serializerOutKey = serializerOutKey;
        this.serializerOutValue = serializerOutValue;
        this.comparator = comparator;
        this.hasher = hasher;
    }

    public Mapper<String, String, K1, V1> getMapper() {
        return mapper;
    }

    public Reducer<K1, V1, K2, V2> getReducer() {
        return reducer;
    }

    public PrecombineFunction<K1, V1, K2, V2> getPrecombineFunction() { return precombineFunction; }

    public Serializer<K1> getSerializerInterKey() {
        return serializerInterKey;
    }

    public Serializer<V1> getSerializerInterValue() {
        return serializerInterValue;
    }

    public Deserializer<K1> getDeserializerInterKey() {
        return deserializerInterKey;
    }

    public Deserializer<V1> getDeserializerInterValue() {
        return deserializerInterValue;
    }

    public Serializer<K2> getSerializerOutKey() {
        return serializerOutKey;
    }

    public Serializer<V2> getSerializerOutValue() {
        return serializerOutValue;
    }

    public Comparator<K1> getComparator() {
        return comparator;
    }

    public KeyHasher<K1> getHasher() {
        return hasher;
    }
}
