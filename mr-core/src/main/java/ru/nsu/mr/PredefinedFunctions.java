package ru.nsu.mr;

import java.util.Comparator;

public class PredefinedFunctions {
    public static Serializer<String> STRING_SERIALIZER = x -> x;
    public static Serializer<Integer> INTEGER_SERIALIZER = Object::toString;
    public static Deserializer<String> STRING_DESERIALIZER = x -> x;
    public static Deserializer<Integer> INTEGER_DESERIALIZER = Integer::parseInt;
    public static Comparator<String> STRING_KEY_COMPARATOR = String::compareTo;
    public static KeyHasher<String> STRING_KEY_HASH = String::hashCode;
}
