package ru.nsu.mr.sources;

import ru.nsu.mr.Pair;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class GroupedKeyValuesIterator<K, V>
        implements Iterator<Pair<K, Iterator<V>>>, AutoCloseableSource {
    private final Iterator<Pair<K, V>> inputIterator;
    private Pair<K, V> currentInputRecord;
    private long currentInputRecordIndex = -1;
    private K currentGroupKey = null;

    public GroupedKeyValuesIterator(Iterator<Pair<K, V>> inputIterator) {
        this.inputIterator = inputIterator;
        moveCurrentInputRecord();
    }

    private void moveCurrentInputRecord() {
        currentInputRecord = inputIterator.hasNext() ? inputIterator.next() : null;
        currentInputRecordIndex += 1;
    }

    private void skipCurrentIterator() {
        while (currentInputRecord != null && currentInputRecord.key().equals(currentGroupKey)) {
            moveCurrentInputRecord();
        }
    }

    @Override
    public boolean hasNext() {
        skipCurrentIterator();
        return currentInputRecord != null;
    }

    @Override
    public Pair<K, Iterator<V>> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        K groupKey = currentInputRecord.key();
        currentGroupKey = groupKey;

        return new Pair<>(
                groupKey,
                new Iterator<>() {
                    private long groupValueIndex = currentInputRecordIndex;

                    @Override
                    public boolean hasNext() {
                        if (groupValueIndex != currentInputRecordIndex) {
                            throw new RuntimeException("This iterator is not current.");
                        }
                        return currentInputRecord != null
                                && groupKey.equals(currentInputRecord.key());
                    }

                    @Override
                    public V next() {
                        if (!hasNext()) {
                            throw new NoSuchElementException();
                        }

                        V result = currentInputRecord.value();
                        moveCurrentInputRecord();
                        groupValueIndex += 1;
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
