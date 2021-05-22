package ru.mail.polis.mityushin;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.DAO;
import ru.mail.polis.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NavigableSet;
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
        return set.tailSet(fromKey(from), true).iterator();
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
        set.remove(fromKey(key));
    }

    @Override
    public void close() throws IOException {
    }

    private Record fromKey(ByteBuffer key) {
        return Record.of(key, EMPTY_VALUE);
    }
}
