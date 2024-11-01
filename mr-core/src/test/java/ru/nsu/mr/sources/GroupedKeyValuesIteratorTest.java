package ru.nsu.mr.sources;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import ru.nsu.mr.Pair;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class GroupedKeyValuesIteratorTest {

    private Iterator<Pair<String, Integer>> inputIterator;

    @BeforeEach
    public void setUp() {
        List<Pair<String, Integer>> inputPairs = new ArrayList<>();
        inputPairs.add(new Pair<>("a", 1));
        inputPairs.add(new Pair<>("a", 2));
        inputPairs.add(new Pair<>("a", 3));
        inputPairs.add(new Pair<>("a", 4));
        inputPairs.add(new Pair<>("b", 3));
        inputPairs.add(new Pair<>("b", 4));
        inputPairs.add(new Pair<>("c", 5));
        inputPairs.add(new Pair<>("c", 6));
        inputPairs.add(new Pair<>("c", 7));
        inputPairs.add(new Pair<>("d", 8));

        inputIterator = inputPairs.iterator();
    }

    @Test
    public void testGroupedKeyValuesIterator() {
        GroupedKeyValuesIterator<String, Integer> groupedIterator =
                new GroupedKeyValuesIterator<>(inputIterator);

        assertTrue(groupedIterator.hasNext());

        Pair<String, Iterator<Integer>> group1 = groupedIterator.next();
        List<Integer> valuesGroup1 = toList(group1.value());
        assertEquals("a", group1.key());
        assertEquals(List.of(1, 2, 3, 4), valuesGroup1);

        assertTrue(groupedIterator.hasNext());

        Pair<String, Iterator<Integer>> group2 = groupedIterator.next();
        List<Integer> valuesGroup2 = toList(group2.value());
        assertEquals("b", group2.key());
        assertEquals(List.of(3, 4), valuesGroup2);

        assertTrue(groupedIterator.hasNext());

        Pair<String, Iterator<Integer>> group3 = groupedIterator.next();
        List<Integer> valuesGroup3 = toList(group3.value());
        assertEquals("c", group3.key());
        assertEquals(List.of(5, 6, 7), valuesGroup3);

        assertTrue(groupedIterator.hasNext());

        Pair<String, Iterator<Integer>> group4 = groupedIterator.next();
        List<Integer> valuesGroup4 = toList(group4.value());
        assertEquals("d", group4.key());
        assertEquals(List.of(8), valuesGroup4);

        assertFalse(groupedIterator.hasNext());
    }

    private List<Integer> toList(Iterator<Integer> iterator) {
        List<Integer> values = new ArrayList<>();
        while (iterator.hasNext()) {
            values.add(iterator.next());
        }
        return values;
    }
}
