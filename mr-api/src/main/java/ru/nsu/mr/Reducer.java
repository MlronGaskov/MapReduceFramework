package ru.nsu.mr;

import java.util.Iterator;

public interface Reducer<KEY_IN, VALUE_IN, KEY_OUT, VALUE_OUT> {
    void reduce(KEY_IN key, Iterator<VALUE_IN> values, OutputContext<KEY_OUT, VALUE_OUT> output);
}
