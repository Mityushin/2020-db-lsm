package ru.mail.polis.mityushin;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.mail.polis.DAO;
import ru.mail.polis.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.TreeSet;

/**
 * Simple in memory storage based on {@link NavigableSet}
 *
 * @author Dmitry Mityushin
 */
public class InMemoryBasedOnNavigableSetDAO implements DAO {

    private static final ByteBuffer EMPTY_VALUE = ByteBuffer.allocate(0);

    private final NavigableSet<Record> set;

    public InMemoryBasedOnNavigableSetDAO() {
        this.set = new TreeSet<>();
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull ByteBuffer from) throws IOException {
        return set.tailSet(toRecord(from), true)
                .iterator();
    }

    @NotNull
    @Override
    public Iterator<Record> range(@NotNull ByteBuffer from, @Nullable ByteBuffer to) throws IOException {
        Record fromElement = toRecord(from);
        NavigableSet<Record> subSet;
        if (to == null) {
            subSet = set.tailSet(fromElement, true);
        } else {
            subSet = set.subSet(fromElement, true, toRecord(to), false);
        }
        return subSet.iterator();
    }

    @NotNull
    @Override
    public ByteBuffer get(@NotNull ByteBuffer key) throws IOException, NoSuchElementException {
        Record elementToFind = toRecord(key);
        Record founded = set.ceiling(elementToFind);
        if (founded == null || founded.compareTo(elementToFind) != 0) {
            throw new NoSuchElementException();
        }
        ByteBuffer value = founded.getValue();
        if (value == null) {
            throw new NoSuchElementException();
        }
        return value;
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) throws IOException {
        Record element = Record.of(key, value);
        while (!set.add(element)) {
            set.remove(element);
        }
    }

    @Override
    public void remove(@NotNull ByteBuffer key) throws IOException {
        set.remove(toRecord(key));
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof InMemoryBasedOnNavigableSetDAO)) return false;

        InMemoryBasedOnNavigableSetDAO that = (InMemoryBasedOnNavigableSetDAO) o;

        return set.equals(that.set);
    }

    @Override
    public int hashCode() {
        return set.hashCode();
    }

    @Override
    public String toString() {
        return "InMemoryBasedOnNavigableSetDAO{" +
                "set=" + set +
                '}';
    }

    private Record toRecord(ByteBuffer key) {
        return Record.of(key, EMPTY_VALUE);
    }
}
