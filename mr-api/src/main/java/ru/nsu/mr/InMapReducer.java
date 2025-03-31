package ru.nsu.mr;

import java.util.Iterator;

public abstract class InMapReducer<KEY_IN, VALUE_IN, KEY_OUT, VALUE_OUT>
        implements Reducer<KEY_IN, VALUE_IN, KEY_OUT, VALUE_OUT>{

    public abstract void generalReduce(KEY_OUT key, Iterator<VALUE_OUT> values, OutputContext<KEY_OUT, VALUE_OUT> output);
    public final boolean altMode() {
        return true;
    }
}
