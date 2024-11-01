package ru.nsu.mr.sources;

import ru.nsu.mr.Pair;

import java.io.IOException;
import java.util.*;

public class MergedKeyValueIterator<K, V> implements Iterator<Pair<K, V>>, AutoCloseableSource {
    private final PriorityQueue<Pair<Pair<K, V>, Integer>> minHeap;
    private final List<Iterator<Pair<K, V>>> iterators;

    public MergedKeyValueIterator(List<Iterator<Pair<K, V>>> iterators, Comparator<K> comparator) {
        this.iterators = iterators;
        this.minHeap =
                new PriorityQueue<>((a, b) -> comparator.compare(a.key().key(), b.key().key()));
        for (int i = 0; i < iterators.size(); i++) {
            if (iterators.get(i).hasNext()) {
                minHeap.add(new Pair<>(iterators.get(i).next(), i));
            }
        }
    }

    @Override
    public boolean hasNext() {
        return !minHeap.isEmpty();
    }

    @Override
    public Pair<K, V> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        Pair<Pair<K, V>, Integer> minNode = minHeap.poll();
        assert minNode != null;
        Pair<K, V> result = minNode.key();
        int index = minNode.value();

        if (iterators.get(index).hasNext()) {
            minHeap.add(new Pair<>(iterators.get(index).next(), index));
        }

        return result;
    }

    @Override
    public void close() throws IOException {
        for (Iterator<Pair<K, V>> iterator : iterators) {
            if (iterator instanceof AutoCloseableSource) {
                ((AutoCloseableSource) iterator).close();
            }
        }
    }
}
