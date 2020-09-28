package ru.mail.polis.dao.re1nex;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.Iters;
import ru.mail.polis.Record;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import static java.util.Objects.requireNonNull;

public class NewDAO implements DAO {
    private static final String SUFFIX = ".dat";
    private static final String TEMP = ".tmp";

    private final File storage;
    private final long flushThreshold;

    //Data
    private MemTable memTable;
    private NavigableMap<Integer, Table> ssTables;

    //State
    private int version;

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
        this.ssTables = new TreeMap<>();
        this.memTable = new MemTable();
        version = -1;
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
                                if (gen > version) {
                                    version = gen;
                                }
                            }
                        }
                );
        version++;
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        final Iterator<Cell> alive = Iterators.filter(cellIterator(from),
                cell -> !requireNonNull(cell).getValue().isTombstone());
        return Iterators.transform(alive, cell -> Record.of(requireNonNull(cell).getKey(), cell.getValue().getData()));
    }

    private Iterator<Cell> cellIterator(@NotNull final ByteBuffer from) throws IOException {
        final List<Iterator<Cell>> iters = new ArrayList<>(ssTables.size() + 1);
        iters.add(memTable.iterator(from));
        ssTables.descendingMap().values().forEach(table -> {
            try {
                iters.add(table.iterator(from));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        final Iterator<Cell> merged = Iterators.mergeSorted(iters, Comparator.naturalOrder());
        return Iters.collapseEquals(merged, Cell::getKey);
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        memTable.upsert(key, value);
        if (memTable.sizeInBytes() > flushThreshold) {
            flush();
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        memTable.remove(key);
        if (memTable.sizeInBytes() > flushThreshold) {
            flush();
        }
    }

    private void flush() throws IOException {
        //Dump memTable
        final File file = new File(storage, version + TEMP);
        SSTable.serialize(file, memTable.iterator(ByteBuffer.allocate(0)));
        final File dst = new File(storage, version + SUFFIX);
        Files.move(file.toPath(), dst.toPath(), StandardCopyOption.ATOMIC_MOVE);

        //Switch
        memTable = new MemTable();
        ssTables.put(version, new SSTable(dst));
        version++;
    }

    @Override
    public void close() throws IOException {
        if (memTable.size() > 0) {
            flush();
        }
        for (int i = 0; i < ssTables.size(); i++) {
            ssTables.get(i).close();
        }

    }

    @Override
    public void compact() throws IOException {
        final Iterator<Cell> iterator = cellIterator(ByteBuffer.allocate(0));
        final File tmp = new File(storage, version + TEMP);
        SSTable.serialize(tmp, iterator);
        for (int i = 0; i < version; i++) {
            Files.delete(new File(storage, i + SUFFIX).toPath());
        }
        version = 0;
        final File file = new File(storage, version + SUFFIX);
        Files.move(tmp.toPath(), file.toPath(), StandardCopyOption.ATOMIC_MOVE);
        ssTables = new TreeMap<>();
        ssTables.put(version, new SSTable(file));
        memTable = new MemTable();
        version++;
    }
}
