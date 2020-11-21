package ru.mail.polis.service;

import org.junit.jupiter.api.Test;
import ru.mail.polis.dao.re1nex.ConsistentHashingTopology;
import ru.mail.polis.dao.re1nex.Topology;
import ru.mail.polis.service.re1nex.ByteBufferUtils;

import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class HashTopologyTest extends ClusterTestBase {
    private static final int NUM_ELEM = 10000000;
    private static final float delta = 0.5f;

    @Override
    int getClusterSize() {
        return 3;
    }

    @Test
    void equalsTest() throws NoSuchAlgorithmException {
        for (final String node : nodes) {
            final Topology<String> first = new ConsistentHashingTopology(endpoints, node);
            final Topology<String> second = new ConsistentHashingTopology(endpoints, node);
            for (int i = 0; i < NUM_ELEM; i++) {
                final ByteBuffer key = randomKeyBuffer();
                final String nodesFirst = first.primaryFor(key);
                final String nodesSecond = second.primaryFor(key);
                assertEquals(nodesFirst, nodesSecond);
            }
        }
    }

    @Test
    void testDistribution() throws NoSuchAlgorithmException {
        for (final String node : nodes) {
            final Map<String, Integer> numElementsForNodesReal = new HashMap<>();
            for (final String n : nodes) {
                numElementsForNodesReal.put(n, 0);
            }
            final Topology<String> first = new ConsistentHashingTopology(endpoints, node);
            final int numElementsForNodes = 2 * NUM_ELEM / nodes.length;
            for (int i = -NUM_ELEM; i < NUM_ELEM; i++) {
                final ByteBuffer key = ByteBufferUtils.getByteBufferKey(String.valueOf(i));
                final String nodesFirst = first.primaryFor(key);
                numElementsForNodesReal.put(nodesFirst,
                        numElementsForNodesReal.get(nodesFirst) + 1);
            }
            for (final int numNodes : numElementsForNodesReal.values()) {
                final int diff = Math.abs(numElementsForNodes - numNodes);
                assertTrue(diff < numElementsForNodes * delta);
            }

        }
    }

}
