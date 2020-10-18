package ru.mail.polis.dao.re1nex;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.Iters;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.util.Objects.requireNonNull;

public class NewDAO implements DAO {
    private static final String SUFFIX = ".dat";
    private static final String TEMP = ".tmp";

    private final File storage;
    private final long flushThreshold;
    //Data
    private TableSet tables;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Realization of LSMDAO.
     *
     * @param storage        - SSTable storage directory
     * @param flushThreshold - max size of MemTable
     */
    public NewDAO(@NotNull final File storage, final long flushThreshold) {
        assert flushThreshold > 0L;
        this.flushThreshold = flushThreshold;
        this.storage = storage;
        final NavigableMap<Integer, Table> ssTables = new ConcurrentSkipListMap<>();
        final AtomicInteger version = new AtomicInteger(-1);
        final File[] list = storage.listFiles((dir1, name) -> name.endsWith(SUFFIX));
        assert list != null;
        Arrays.stream(list)
                .filter(currentFile -> !currentFile.isDirectory())
                .forEach(f -> {
                            final String name = f.getName();
                            final String sub = name.substring(0, name.indexOf(SUFFIX));
                            if (sub.matches("[0-9]+")) {
                                final int gen =
                                        Integer.parseInt(sub);
                                try {
                                    ssTables.put(gen, new SSTable(f));
                                } catch (IOException e) {
                                    throw new UncheckedIOException(e);
                                }
                                if (gen > version.get()) {
                                    version.set(gen);
                                }
                            }
                        }
                );
        tables = new TableSet(ssTables, version.addAndGet(1));
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        lock.readLock().lock();
        try {
            final Iterator<Cell> alive = Iterators.filter(cellIterator(from),
                    cell -> !requireNonNull(cell).getValue().isTombstone());
            return Iterators.transform(alive,
                    cell -> Record.of(requireNonNull(cell).getKey(),
                            cell.getValue().getData()));
        } finally {
            lock.readLock().unlock();
        }
    }

    private Iterator<Cell> cellIterator(@NotNull final ByteBuffer from) throws IOException {
        final TableSet snapshot;
        lock.readLock().lock();
        try {
            snapshot = this.tables;
            final List<Iterator<Cell>> iters = new ArrayList<>(snapshot.ssTables.size() + snapshot.flushing.size() + 1);
            iters.add(snapshot.memTable.iterator(from));
            snapshot.ssTables.descendingMap().values().forEach(table -> {
                try {
                    iters.add(table.iterator(from));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            final Iterator<Cell> merged = Iterators.mergeSorted(iters, Comparator.naturalOrder());
            return Iters.collapseEquals(merged, Cell::getKey);
        } finally {
            lock.readLock().unlock();
        }

    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        final boolean needsFlush;
        lock.readLock().lock();
        try {
            needsFlush = tables.memTable.sizeInBytes() >= flushThreshold;
            tables.memTable.upsert(key, value);
        } finally {
            lock.readLock().unlock();
        }
        if (needsFlush) {
            flush();
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        final boolean needsFlush;
        lock.readLock().lock();
        try {
            needsFlush = tables.memTable.sizeInBytes() >= flushThreshold;
            tables.memTable.remove(key);
        } finally {
            lock.readLock().unlock();
        }
        if (needsFlush) {
            flush();
        }
    }

    private void flush() throws IOException {
        final TableSet snapshot;
        lock.writeLock().lock();
        try {
            snapshot = tables;
            if (snapshot.memTable.sizeInBytes() == 0L) {
                return;
            }
            tables = tables.addMemTableToFlushing(snapshot.flushing);
        } finally {
            lock.writeLock().unlock();
        }
        //Dump memTable
        lock.writeLock().lock();
        try {
            final File file = new File(storage, snapshot.version + TEMP);
            SSTable.serialize(file, snapshot.memTable.iterator(ByteBuffer.allocate(0)));
            final File dst = new File(storage, snapshot.version + SUFFIX);
            Files.move(file.toPath(), dst.toPath(), StandardCopyOption.ATOMIC_MOVE);
            tables = tables.addFlushingToSSTable(snapshot.memTable, tables.flushing, new SSTable(dst));
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {
        if (tables.memTable.size() > 0) {
            flush();
        }
        for (int i = 0; i < tables.ssTables.size(); i++) {
            if (tables.ssTables.get(i) != null) {
                tables.ssTables.get(i).close();
            }
        }

    }

    @Override
    public void compact() throws IOException {
        final TableSet snapshot;

        lock.readLock().lock();
        try {
            snapshot = this.tables;
        } finally {
            lock.readLock().unlock();
        }
        final ByteBuffer from = ByteBuffer.allocate(0);
        lock.writeLock().lock();
        try {
            final Collection<Iterator<Cell>> iterators = new ArrayList<>(snapshot.ssTables.size());
            snapshot.ssTables.descendingMap().values().forEach(table -> {
                try {
                    iterators.add(table.iterator(from));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            final Iterator<Cell> merged = Iterators.mergeSorted(iterators, Comparator.naturalOrder());
            final Iterator<Cell> iterator = Iters.collapseEquals(merged, Cell::getKey);
            if (iterator.hasNext()) {
                final File tmp = new File(storage, tables.version + TEMP);
                SSTable.serialize(tmp, iterator);
                for (int i = 0; i < snapshot.version; i++) {
                    final File file = new File(storage, i + SUFFIX);
                    if (file.exists()) {
                        Files.delete(file.toPath());
                    }
                }
                snapshot.version = 0;
                final File file = new File(storage, snapshot.version + SUFFIX);
                Files.move(tmp.toPath(), file.toPath(), StandardCopyOption.ATOMIC_MOVE);
                snapshot.ssTables.clear();
                snapshot.ssTables.put(snapshot.version, new SSTable(file));
                tables = snapshot.compact(tables.memTable, new SSTable(file));
            }
        } finally {
            lock.writeLock().unlock();
        }
    }
}
