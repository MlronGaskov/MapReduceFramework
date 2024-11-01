package ru.nsu.mr.sources;

import ru.nsu.mr.Pair;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class GroupedKeyValuesIterator<K, V>
        implements Iterator<Pair<K, Iterator<V>>>, AutoCloseableSource {
    private final Iterator<Pair<K, V>> inputIterator;
    private Pair<K, V> currentInputRecord;

    public GroupedKeyValuesIterator(Iterator<Pair<K, V>> inputIterator) {
        this.inputIterator = inputIterator;
        if (inputIterator.hasNext()) {
            currentInputRecord = inputIterator.next();
        }
    }

    @Override
    public boolean hasNext() {
        return currentInputRecord != null;
    }

    @Override
    public Pair<K, Iterator<V>> next() {
        // TODO: roll current group iterator till the end of the group
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        K groupKey = currentInputRecord.key();

        return new Pair<>(
                groupKey,
                new Iterator<>() {
                    private V groupCurrentValue = currentInputRecord.value();

                    @Override
                    public boolean hasNext() {
                        return groupCurrentValue != null;
                    }

                    @Override
                    public V next() {
                        if (!hasNext()) {
                            throw new NoSuchElementException();
                        }

                        V result = groupCurrentValue;
                        groupCurrentValue = null;
                        currentInputRecord = inputIterator.hasNext() ? inputIterator.next() : null;
                        if (currentInputRecord != null
                                && currentInputRecord.key().equals(groupKey)) {
                            groupCurrentValue = currentInputRecord.value();
                        }
                        return result;
                    }
                });
    }

    @Override
    public void close() throws IOException {
        if (inputIterator instanceof AutoCloseableSource) {
            ((AutoCloseableSource) inputIterator).close();
        }
    }
}
