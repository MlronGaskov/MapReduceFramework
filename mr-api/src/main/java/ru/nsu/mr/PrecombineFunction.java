package ru.nsu.mr;

import java.util.Iterator;

public interface PrecombineFunction<KEY_IN, VALUE_IN, KEY_OUT, VALUE_OUT> {
    void precombine(KEY_IN key, Iterator<VALUE_IN> values, OutputContext<KEY_OUT, VALUE_OUT> output);
}
