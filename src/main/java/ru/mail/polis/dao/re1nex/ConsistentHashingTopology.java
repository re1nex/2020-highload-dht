package ru.mail.polis.dao.re1nex;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import static java.nio.charset.StandardCharsets.UTF_8;

public class ConsistentHashingTopology implements Topology<String> {

    private static final int NUM_VIRTUAL_NODES = 5;
    @NotNull
    private final String local;
    @NotNull
    private final SortedMap<Long, String> map = new TreeMap<>();
    @NotNull
    private final MD5Hash md5Hash = new MD5Hash();
    private final int uniqueSize;

    /**
     * Provides topology by consistent hashing.
     *
     * @param nodes - set of nodes
     * @param local - current node
     */
    public ConsistentHashingTopology(
            @NotNull final Collection<String> nodes,
            @NotNull final String local) throws NoSuchAlgorithmException {
        this.local = local;
        for (final String node : nodes) {
            for (int i = 0; i < NUM_VIRTUAL_NODES; i++) {
                final String newHash = node + i;
                final long hash = md5Hash.calculateHash(newHash.getBytes(UTF_8));
                map.put(hash, node);
            }
        }
        uniqueSize = nodes.size();
    }

    @Override
    public boolean isLocal(@NotNull final String node) {
        return node.equals(local);
    }

    @Override
    public boolean removeLocal(@NotNull Set<String> nodes) {
        return nodes.remove(local);
    }

    @Override
    public int getUniqueSize() {
        return uniqueSize;
    }

    @NotNull
    @Override
    public String primaryFor(@NotNull final ByteBuffer key) throws NoSuchAlgorithmException {
        final byte[] keyByte = new byte[key.remaining()];
        long hash = md5Hash.calculateHash(keyByte);
        final SortedMap<Long, String> tailMap = map.tailMap(hash);
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

    @NotNull
    @Override
    public Set<String> severalNodesForKey(@NotNull ByteBuffer key, int numNodes) throws NoSuchAlgorithmException {
        final byte[] keyByte = new byte[key.remaining()];
        long hash = md5Hash.calculateHash(keyByte);
        final SortedMap<Long, String> tailMap = map.tailMap(hash);
        final SortedSet<String> nodes = new TreeSet<>();
        getNodesFromMap(tailMap, nodes, numNodes);
        getNodesFromMap(map, nodes, numNodes);
        return nodes;
    }

    private void getNodesFromMap(@NotNull final Map<Long, String> src,
                                 @NotNull final Set<String> dst,
                                 final int numNodes) {
        for (final String item : src.values()) {
            if (dst.size() == numNodes) {
                return;
            }
            dst.add(item);
        }
    }

    private static class MD5Hash {

        long calculateHash(final byte[] key) throws NoSuchAlgorithmException {
            final MessageDigest instance = MessageDigest.getInstance("MD5");
            instance.reset();
            instance.update(key);
            final byte[] digest = instance.digest();

            long h = 0;
            for (int i = 0; i < 4; i++) {
                h <<= 8;
                h |= ((int) digest[i]) & 0xFF;
            }
            return h;
        }
    }
}
