package ru.nsu.mr;

public abstract class AfterMapReducer<KEY_IN, VALUE_IN, KEY_OUT, VALUE_OUT>
        implements Reducer<KEY_IN, VALUE_IN, KEY_OUT, VALUE_OUT> {

    public final boolean altMode() {
        return false;
    }
}
