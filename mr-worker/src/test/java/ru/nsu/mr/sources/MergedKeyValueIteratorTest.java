package ru.nsu.mr.sources;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import ru.nsu.mr.Pair;

import java.util.*;

class MergedKeyValueIteratorTest {
    private List<Iterator<Pair<String, Integer>>> iterators;

    @BeforeEach
    public void setUp() {
        Iterator<Pair<String, Integer>> it1 =
                Arrays.asList(
                                new Pair<>("apple", 0),
                                new Pair<>("apple", 0),
                                new Pair<>("banana", 0))
                        .iterator();

        Iterator<Pair<String, Integer>> it2 =
                Arrays.asList(new Pair<>("cherry", 4), new Pair<>("date", 0), new Pair<>("date", 0))
                        .iterator();

        Iterator<Pair<String, Integer>> it3 =
                Arrays.asList(new Pair<>("banana", 0), new Pair<>("fig", 5)).iterator();

        iterators = Arrays.asList(it1, it2, it3);
    }

    @Test
    public void testMergedKeyValueIterator() {
        Comparator<String> comparator = Comparator.naturalOrder();
        MergedKeyValueIterator<String, Integer> mergedIterator =
                new MergedKeyValueIterator<>(iterators, comparator);

        List<Pair<String, Integer>> expected =
                Arrays.asList(
                        new Pair<>("apple", 0),
                        new Pair<>("apple", 0),
                        new Pair<>("banana", 0),
                        new Pair<>("banana", 0),
                        new Pair<>("cherry", 4),
                        new Pair<>("date", 0),
                        new Pair<>("date", 0),
                        new Pair<>("fig", 5));

        int index = 0;
        while (mergedIterator.hasNext()) {
            Pair<String, Integer> pair = mergedIterator.next();
            assertEquals(expected.get(index), pair);
            index++;
        }
    }
}
