package ru.nsu.mr;


import java.util.Comparator;
import java.util.List;

public class PredefinedFunctions {
    public static Serializer<String> STRING_SERIALIZER = x -> x;
    public static Serializer<Integer> INTEGER_SERIALIZER = Object::toString;
    public static Deserializer<String> STRING_DESERIALIZER = x -> x;
    public static Deserializer<Integer> INTEGER_DESERIALIZER = Integer::parseInt;
    public static Comparator<String> STRING_KEY_COMPARATOR = String::compareTo;
    public static KeyHasher<String> STRING_KEY_HASH = String::hashCode;

    public static Serializer<List<String>> LIST_SERIALIZER = list ->
            String.join(",", list); // Преобразуем список в строку через запятую

    public static Deserializer<List<String>> LIST_DESERIALIZER = str ->
            List.of(str.split(",")); // Преобразуем строку обратно в список по запятой
}

