package ru.mail.polis.dao.re1nex;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.SortedMap;
import java.util.TreeMap;

import static java.nio.charset.StandardCharsets.UTF_8;

public class ConsistentHashingTopology implements Topology<String> {

    private static final int NUM_VIRTUAL_NODES = 5;
    @NotNull
    private final String local;
    @NotNull
    private final SortedMap<Long, String> map = new TreeMap<>();
    @NotNull
    private final MD5Hash hashFunc = new MD5Hash();

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
                map.put(hashFunc.hash(newHash.getBytes(UTF_8)), node);
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
        final byte[] keyByte = new byte[key.remaining()];
        long hash = hashFunc.hash(keyByte);
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

    private static class MD5Hash {
        MessageDigest instance;

        MD5Hash() throws NoSuchAlgorithmException {
            instance = MessageDigest.getInstance("MD5");
        }

        long hash(byte[] key) {
            instance.reset();
            instance.update(key);
            byte[] digest = instance.digest();

            long h = 0;
            for (int i = 0; i < 4; i++) {
                h <<= 8;
                h |= ((int) digest[i]) & 0xFF;
            }
            return h;
        }
    }

}
