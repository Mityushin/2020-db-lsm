package ru.mail.polis.mityushin;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.mail.polis.DAO;
import ru.mail.polis.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Simple in memory storage based on {@link SortedMap}
 *
 * @author Dmitry Mityushin
 */
public class InMemoryBasedOnSortedMapDAO implements DAO {

    private final SortedMap<ByteBuffer, Record> map;

    public InMemoryBasedOnSortedMapDAO() {
        this.map = new TreeMap<>();
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull ByteBuffer from) throws IOException {
        return map.tailMap(from)
                .values()
                .iterator();
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) throws IOException {
        map.compute(key, (k, v) -> Record.of(key, value));
    }

    @Override
    public void remove(@NotNull ByteBuffer key) throws IOException {
        map.remove(key);
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof InMemoryBasedOnSortedMapDAO)) return false;

        InMemoryBasedOnSortedMapDAO that = (InMemoryBasedOnSortedMapDAO) o;

        return map.equals(that.map);
    }

    @Override
    public int hashCode() {
        return map.hashCode();
    }

    @Override
    public String toString() {
        return "InMemoryBasedOnSortedMapDAO{" +
                "map=" + map +
                '}';
    }
}
