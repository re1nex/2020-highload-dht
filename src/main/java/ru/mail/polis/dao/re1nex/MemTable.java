package ru.mail.polis.dao.re1nex;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

final class MemTable implements Table {

    private final SortedMap<ByteBuffer, Value> map = new ConcurrentSkipListMap<>();
    private final AtomicLong size = new AtomicLong();

    MemTable() {
        size.set(720L);
    }

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull final ByteBuffer from) throws IOException {
        return map.tailMap(from)
                .entrySet()
                .stream()
                .map(element -> new Cell(element.getKey(), element.getValue()))
                .iterator();
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        if (map.containsKey(key)) {
            size.set(size.get() + value.remaining() + Long.BYTES);
        } else {
            size.set(size.get() + value.remaining() + key.remaining() + Long.BYTES);
        }
        map.put(key.duplicate(), new Value(System.currentTimeMillis(), value.duplicate()));
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        final Value previous = map.put(key.duplicate(), new Value(System.currentTimeMillis()));
        if (previous == null) {
            size.set(size.get() + key.remaining());
        } else if (!previous.isTombstone()) {
            size.set(size.get() - previous.getData().remaining());
        }
    }

    @Override
    public void close() throws IOException {
        map.clear();
    }

    int size() {
        return map.size();
    }

    long sizeInBytes() {
        return size.get();
    }
}
