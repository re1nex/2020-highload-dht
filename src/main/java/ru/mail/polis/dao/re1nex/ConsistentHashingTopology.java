package ru.mail.polis.dao.re1nex;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.SortedMap;
import java.util.TreeMap;

public class ConsistentHashingTopology implements Topology<String> {

    private static final int NUM_VIRTUAL_NODES = 5;
    @NotNull
    private final String local;
    @NotNull
    private final SortedMap<Integer, String> map = new TreeMap<>();

    /**
     * Provides topology by consistent hashing.
     *
     * @param nodes - set of nodes
     * @param local - current node
     */
    public ConsistentHashingTopology(
            @NotNull final Collection<String> nodes,
            @NotNull final String local) {
        this.local = local;
        for (final String node : nodes) {
            for (int i = 0; i < NUM_VIRTUAL_NODES; i++) {
                map.put((node + i).hashCode(), node);
            }
        }
    }

    @Override
    public boolean isLocal(@NotNull final String node) {
        return node.equals(local);
    }

    @NotNull
    @Override
    public String primaryFor(@NotNull final ByteBuffer key) {
        int hash = key.hashCode();
        final SortedMap<Integer, String> tailMap = map.tailMap(hash);
        hash = tailMap.isEmpty() ? map.firstKey() : tailMap.firstKey();
        return map.get(hash);
    }

    @Override
    public int size() {
        return map.size();
    }

    @NotNull
    @Override
    public String[] all() {
        return map.values()
                .stream()
                .distinct()
                .toArray(String[]::new);
    }
}
