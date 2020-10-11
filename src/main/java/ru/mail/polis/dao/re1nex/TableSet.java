package ru.mail.polis.dao.re1nex;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

public class TableSet {
    public final NavigableMap<Integer, Table> ssTables;
    public final Set<Table> flushing;
    public final MemTable memTable;
    public int version;
    private static final Logger logger = LoggerFactory.getLogger(NewDAO.class);

    /**
     * Contains all necessary information for dao work
     */
    public TableSet(@NotNull final MemTable memTable,
                    @NotNull final Set<Table> flushing,
                    @NotNull final NavigableMap<Integer, Table> ssTables,
                    final int version) {
        assert version >= 0;
        this.ssTables = ssTables;
        this.flushing = Collections.unmodifiableSet(flushing);
        this.memTable = memTable;
        this.version = version;
    }

    /**
     * Contains all necessary information for dao work
     * need for first initialization
     */
    public TableSet(@NotNull final NavigableMap<Integer, Table> ssTables,
                    final int version) {
        assert version >= 0;
        this.ssTables = ssTables;
        this.flushing = new HashSet<>();
        this.memTable = new MemTable();
        this.version = version;
    }

    @NotNull
    TableSet addMemTableToFlushing(@NotNull final Set<Table> flushing) {
        final Set<Table> flush = new HashSet<>(flushing);
        flush.add(memTable);
        return new TableSet(new MemTable(), flush, ssTables, ++version);
    }

    @NotNull
    TableSet addFlushingToSSTable(@NotNull final MemTable deleteMem,
                                  @NotNull final Set<Table> flushing,
                                  @NotNull final SSTable ssTable) {
        final Set<Table> flush = new HashSet<>(flushing);
        final NavigableMap<Integer, Table> files = new TreeMap<>(this.ssTables);
        if (flush.remove(deleteMem)) {
            files.put(version, ssTable);
        } else {
            logger.error("Can't remove this table");
        }
        return new TableSet(memTable, flush, files, version);
    }

    @NotNull
    TableSet compact(@NotNull final MemTable memTable, @NotNull final SSTable sstable) {
        final NavigableMap<Integer, Table> files = new TreeMap<>();
        files.put(version, sstable);
        version = 1;
        return new TableSet(memTable, flushing, files, version);
    }
}