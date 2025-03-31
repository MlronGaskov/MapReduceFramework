package ru.nsu.mr;

import java.util.Comparator;

public class MapReduceJob<K1, V1, K2, V2> {
    private final Mapper<String, String, K1, V1> mapper;
    private final Reducer<K1, V1, K2, V2> reducer;
    private final Serializer<K1> serializerInterKey;
    private final Serializer<V1> serializerInterValue;
    private final Deserializer<K1> deserializerInterKey;
    private final Deserializer<V1> deserializerInterValue;
    private final Serializer<K2> serializerOutKey;
    private final Serializer<V2> serializerOutValue;
    private final Deserializer<K2> deserializerOutKey;
    private final Deserializer<V2> deserializerOutValue;
    private final Comparator<K1> interComparator;
    private final Comparator<K2> outComparator;
    private final KeyHasher<K1> hasher;

    public MapReduceJob(
            Mapper<String, String, K1, V1> mapper,
            Reducer<K1, V1, K2, V2> reducer,
            Serializer<K1> serializerInterKey,
            Serializer<V1> serializerInterValue,
            Deserializer<K1> deserializerInterKey,
            Deserializer<V1> deserializerInterValue,
            Serializer<K2> serializerOutKey,
            Serializer<V2> serializerOutValue,
            Deserializer<K2> deserializerOutKey,
            Deserializer<V2> deserializerOutValue,
            Comparator<K1> interComparator,
            Comparator<K2> outComparator,
            KeyHasher<K1> hasher) {
        this.mapper = mapper;
        this.reducer = reducer;
        this.serializerInterKey = serializerInterKey;
        this.serializerInterValue = serializerInterValue;
        this.deserializerInterKey = deserializerInterKey;
        this.deserializerInterValue = deserializerInterValue;
        this.serializerOutKey = serializerOutKey;
        this.serializerOutValue = serializerOutValue;
        this.deserializerOutKey = deserializerOutKey;
        this.deserializerOutValue = deserializerOutValue;
        this.interComparator = interComparator;
        this.outComparator = outComparator;
        this.hasher = hasher;
    }

    public Mapper<String, String, K1, V1> getMapper() {
        return mapper;
    }

    public Reducer<K1, V1, K2, V2> getReducer() {
        return reducer;
    }

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

    public Deserializer<K2> getDeserializerOutKey() { return deserializerOutKey; }

    public  Deserializer<V2> getDeserializerOutValue() { return deserializerOutValue; }

    public Comparator<K1> getInterComparator() {
        return interComparator;
    }

    public Comparator<K2> getOutComparator() { return outComparator; }

    public KeyHasher<K1> getHasher() {
        return hasher;
    }
}
