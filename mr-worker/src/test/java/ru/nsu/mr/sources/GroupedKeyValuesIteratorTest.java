package ru.nsu.mr.sources;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import ru.nsu.mr.Pair;

import java.io.IOException;
import java.util.*;

public class GroupedKeyValuesIteratorTest {

    @Test
    void testEmptyInput() {
        Iterator<Pair<String, Integer>> emptyIterator = Collections.emptyIterator();
        try (GroupedKeyValuesIterator<String, Integer> groupedIterator =
                new GroupedKeyValuesIterator<>(emptyIterator)) {
            assertFalse(groupedIterator.hasNext());
            assertThrows(NoSuchElementException.class, groupedIterator::next);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testSingleGroupSingleElement() {
        Iterator<Pair<String, Integer>> inputIterator = List.of(new Pair<>("group1", 1)).iterator();

        try (GroupedKeyValuesIterator<String, Integer> groupedIterator =
                new GroupedKeyValuesIterator<>(inputIterator)) {

            assertTrue(groupedIterator.hasNext());

            Pair<String, Iterator<Integer>> group = groupedIterator.next();
            assertEquals("group1", group.key());
            assertTrue(group.value().hasNext());
            assertEquals(1, group.value().next());
            assertFalse(group.value().hasNext());

            assertFalse(groupedIterator.hasNext());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testSingleGroupMultipleElements() {
        Iterator<Pair<String, Integer>> inputIterator =
                List.of(new Pair<>("group1", 1), new Pair<>("group1", 2), new Pair<>("group1", 3))
                        .iterator();

        try (GroupedKeyValuesIterator<String, Integer> groupedIterator =
                new GroupedKeyValuesIterator<>(inputIterator)) {

            assertTrue(groupedIterator.hasNext());

            Pair<String, Iterator<Integer>> group = groupedIterator.next();
            assertEquals("group1", group.key());

            List<Integer> values = new ArrayList<>();
            group.value().forEachRemaining(values::add);
            assertEquals(List.of(1, 2, 3), values);

            assertFalse(groupedIterator.hasNext());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testMultipleGroups() {
        Iterator<Pair<String, Integer>> inputIterator =
                List.of(
                                new Pair<>("group1", 1),
                                new Pair<>("group1", 2),
                                new Pair<>("group2", 3),
                                new Pair<>("group2", 4),
                                new Pair<>("group3", 5))
                        .iterator();

        try (GroupedKeyValuesIterator<String, Integer> groupedIterator =
                new GroupedKeyValuesIterator<>(inputIterator)) {

            assertTrue(groupedIterator.hasNext());

            Pair<String, Iterator<Integer>> group1 = groupedIterator.next();
            assertEquals("group1", group1.key());
            assertEquals(List.of(1, 2), toList(group1.value()));

            Pair<String, Iterator<Integer>> group2 = groupedIterator.next();
            assertEquals("group2", group2.key());
            assertEquals(List.of(3, 4), toList(group2.value()));

            Pair<String, Iterator<Integer>> group3 = groupedIterator.next();
            assertEquals("group3", group3.key());
            assertEquals(List.of(5), toList(group3.value()));

            assertFalse(groupedIterator.hasNext());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testInvalidInternalIteratorUsage() {
        Iterator<Pair<String, Integer>> inputIterator =
                List.of(new Pair<>("group1", 1), new Pair<>("group1", 2), new Pair<>("group2", 3))
                        .iterator();

        Iterator<Integer> group1Values;
        try (GroupedKeyValuesIterator<String, Integer> groupedIterator =
                new GroupedKeyValuesIterator<>(inputIterator)) {

            Pair<String, Iterator<Integer>> group1 = groupedIterator.next();
            group1Values = group1.value();

            assertEquals(1, group1Values.next());
            groupedIterator.next();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        assertThrows(RuntimeException.class, group1Values::next);
    }

    @Test
    void testSkipCurrentGroup() {
        Iterator<Pair<String, Integer>> inputIterator =
                List.of(
                                new Pair<>("group1", 1),
                                new Pair<>("group1", 2),
                                new Pair<>("group2", 3),
                                new Pair<>("group2", 4))
                        .iterator();
        Pair<String, Iterator<Integer>> group2;
        try (GroupedKeyValuesIterator<String, Integer> groupedIterator =
                new GroupedKeyValuesIterator<>(inputIterator)) {

            assertTrue(groupedIterator.hasNext());
            Pair<String, Iterator<Integer>> group1 = groupedIterator.next();
            assertEquals("group1", group1.key());
            assertTrue(groupedIterator.hasNext());
            assertThrows(RuntimeException.class, group1.value()::hasNext);
            assertThrows(RuntimeException.class, group1.value()::next);
            group2 = groupedIterator.next();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        assertEquals("group2", group2.key());
        assertEquals(List.of(3, 4), toList(group2.value()));
    }

    @Test
    void testSkipPartOfFirstGroup() {
        Iterator<Pair<String, Integer>> inputIterator =
                List.of(
                                new Pair<>("group1", 1),
                                new Pair<>("group1", 2),
                                new Pair<>("group1", 3),
                                new Pair<>("group2", 4),
                                new Pair<>("group2", 5))
                        .iterator();

        Pair<String, Iterator<Integer>> group2;
        try (GroupedKeyValuesIterator<String, Integer> groupedIterator =
                new GroupedKeyValuesIterator<>(inputIterator)) {

            assertTrue(groupedIterator.hasNext());
            Pair<String, Iterator<Integer>> group1 = groupedIterator.next();
            assertEquals("group1", group1.key());
            assertTrue(group1.value().hasNext());
            assertEquals(1, group1.value().next());

            assertTrue(groupedIterator.hasNext());
            group2 = groupedIterator.next();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        assertEquals("group2", group2.key());

        List<Integer> group2Values = new ArrayList<>();
        group2.value().forEachRemaining(group2Values::add);
        assertEquals(List.of(4, 5), group2Values);
    }

    @Test
    void testSkipEntireGroup() {
        Iterator<Pair<String, Integer>> inputIterator =
                List.of(
                                new Pair<>("group1", 1),
                                new Pair<>("group1", 2),
                                new Pair<>("group2", 3),
                                new Pair<>("group2", 4),
                                new Pair<>("group3", 5))
                        .iterator();

        Pair<String, Iterator<Integer>> group3;
        try (GroupedKeyValuesIterator<String, Integer> groupedIterator =
                new GroupedKeyValuesIterator<>(inputIterator)) {

            assertTrue(groupedIterator.hasNext());
            groupedIterator.next();

            assertTrue(groupedIterator.hasNext());
            Pair<String, Iterator<Integer>> group2 = groupedIterator.next();
            assertEquals("group2", group2.key());

            List<Integer> group2Values = new ArrayList<>();
            group2.value().forEachRemaining(group2Values::add);
            assertEquals(List.of(3, 4), group2Values);

            assertTrue(groupedIterator.hasNext());
            group3 = groupedIterator.next();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        assertEquals("group3", group3.key());
        assertEquals(List.of(5), toList(group3.value()));
    }

    @Test
    void testInterleavedReadingAndSkipping() {
        Iterator<Pair<String, Integer>> inputIterator =
                List.of(
                                new Pair<>("group1", 1),
                                new Pair<>("group1", 2),
                                new Pair<>("group2", 3),
                                new Pair<>("group2", 4),
                                new Pair<>("group3", 5),
                                new Pair<>("group3", 6))
                        .iterator();

        Pair<String, Iterator<Integer>> group3;
        try (GroupedKeyValuesIterator<String, Integer> groupedIterator =
                new GroupedKeyValuesIterator<>(inputIterator)) {

            assertTrue(groupedIterator.hasNext());
            Pair<String, Iterator<Integer>> group1 = groupedIterator.next();
            assertEquals("group1", group1.key());
            assertTrue(group1.value().hasNext());
            assertEquals(1, group1.value().next());

            assertTrue(groupedIterator.hasNext());
            groupedIterator.next();

            assertTrue(groupedIterator.hasNext());
            group3 = groupedIterator.next();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        assertEquals("group3", group3.key());
        List<Integer> group3Values = new ArrayList<>();
        group3.value().forEachRemaining(group3Values::add);
        assertEquals(List.of(5, 6), group3Values);
    }

    @Test
    void testSkipToLastGroup() {
        Iterator<Pair<String, Integer>> inputIterator =
                List.of(
                                new Pair<>("group1", 1),
                                new Pair<>("group1", 2),
                                new Pair<>("group2", 3),
                                new Pair<>("group2", 4),
                                new Pair<>("group3", 5))
                        .iterator();

        try (GroupedKeyValuesIterator<String, Integer> groupedIterator =
                new GroupedKeyValuesIterator<>(inputIterator)) {

            groupedIterator.next();
            groupedIterator.next();

            assertTrue(groupedIterator.hasNext());
            Pair<String, Iterator<Integer>> group3 = groupedIterator.next();
            assertEquals("group3", group3.key());
            List<Integer> group3Values = new ArrayList<>();
            group3.value().forEachRemaining(group3Values::add);
            assertEquals(List.of(5), group3Values);

            assertFalse(groupedIterator.hasNext());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testPartialFirstAndLastGroup() {
        Iterator<Pair<String, Integer>> inputIterator =
                List.of(
                                new Pair<>("group1", 1),
                                new Pair<>("group1", 2),
                                new Pair<>("group2", 3),
                                new Pair<>("group3", 4),
                                new Pair<>("group3", 5))
                        .iterator();

        Pair<String, Iterator<Integer>> group3;
        try (GroupedKeyValuesIterator<String, Integer> groupedIterator =
                new GroupedKeyValuesIterator<>(inputIterator)) {

            assertTrue(groupedIterator.hasNext());
            Pair<String, Iterator<Integer>> group1 = groupedIterator.next();
            assertEquals("group1", group1.key());
            assertTrue(group1.value().hasNext());
            assertEquals(1, group1.value().next());

            groupedIterator.next();

            assertTrue(groupedIterator.hasNext());
            group3 = groupedIterator.next();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        assertEquals("group3", group3.key());
        List<Integer> group3Values = new ArrayList<>();
        group3.value().forEachRemaining(group3Values::add);
        assertEquals(List.of(4, 5), group3Values);
    }

    private <T> List<T> toList(Iterator<T> iterator) {
        List<T> result = new ArrayList<>();
        iterator.forEachRemaining(result::add);
        return result;
    }
}
