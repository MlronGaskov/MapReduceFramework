package ru.nsu.mr;

import java.util.Iterator;

public interface Mapper<KEY_IN, VALUE_IN, KEY_OUT, VALUE_OUT> {
    void map(Iterator<Pair<KEY_IN, VALUE_IN>> input, OutputContext<KEY_OUT, VALUE_OUT> output);
}
