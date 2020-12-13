package ru.mail.polis.service;

import org.junit.jupiter.api.Test;
import ru.mail.polis.dao.re1nex.ConsistentHashingTopology;
import ru.mail.polis.dao.re1nex.Topology;

import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class HashTopologyTest extends ClusterTestBase {
    private static final int NUM_ELEM = 10000000;
    private static final float DELTA = 0.5f;

    @Override
    int getClusterSize() {
        return 3;
    }

    @Test
    void equalsTest() throws NoSuchAlgorithmException {
        final String node = nodes[0];
        final Topology<String> first = new ConsistentHashingTopology(endpoints, node);
        final Topology<String> second = new ConsistentHashingTopology(endpoints, node);
        for (int i = 0; i < NUM_ELEM; i++) {
            final ByteBuffer key = randomKeyBuffer();
            final String nodesFirst = first.primaryFor(key);
            final String nodesSecond = second.primaryFor(key);
            assertEquals(nodesFirst, nodesSecond);
        }

    }

    @Test
    void testDistribution() throws NoSuchAlgorithmException {
        final String node = nodes[0];
        final Map<String, Integer> numElementsForNodesReal = new HashMap<>();
        for (final String n : nodes) {
            numElementsForNodesReal.put(n, 0);
        }
        final Topology<String> first = new ConsistentHashingTopology(endpoints, node);
        final int numElementsForNodes = NUM_ELEM / nodes.length;
        for (int i = 0; i < NUM_ELEM; i++) {
            final ByteBuffer key = randomKeyBuffer();
            final String nodesFirst = first.primaryFor(key);
            numElementsForNodesReal.put(nodesFirst,
                    numElementsForNodesReal.get(nodesFirst) + 1);
        }
        for (final int numNodes : numElementsForNodesReal.values()) {
            final int diff = Math.abs(numElementsForNodes - numNodes);
            assertTrue(diff < numElementsForNodes * DELTA);
        }

    }

    @Test
    void testDistributionSeveral() throws NoSuchAlgorithmException {
        final int numReplicas = 2;
        final String node = nodes[0];
        final Topology<String> first = new ConsistentHashingTopology(endpoints, node);
        final Map<String, Integer> numElementsForNodesReal = new HashMap<>();
        for (final String n : nodes) {
            numElementsForNodesReal.put(n, 0);
        }
        final int numElementsForNodes = NUM_ELEM / nodes.length * numReplicas;
        for (int i = 0; i < NUM_ELEM; i++) {
            final ByteBuffer key = randomKeyBuffer();
            final Set<String> nodesForKey = first.severalNodesForKey(key, numReplicas);
            for (final String nodesFirst : nodesForKey) {
                numElementsForNodesReal.put(nodesFirst,
                        numElementsForNodesReal.get(nodesFirst) + 1);
            }
        }
        for (final int numNodes : numElementsForNodesReal.values()) {
            final int diff = Math.abs(numElementsForNodes - numNodes);
            assertTrue(diff < numElementsForNodes * DELTA);
        }
    }

    @Test
    void addNodeTest() throws NoSuchAlgorithmException {
        final String node = nodes[0];
        final Set<String> full = new HashSet<>(endpoints);
        final Set<String> withoutOne = new HashSet<>(endpoints);
        withoutOne.remove(nodes[1]);
        final Topology<String> topologyWithoutOne = new ConsistentHashingTopology(withoutOne, node);
        final Topology<String> topologyFull = new ConsistentHashingTopology(full, node);
        int migration = 0;
        final int numElementsForNodes = NUM_ELEM / nodes.length;
        for (int i = 0; i < NUM_ELEM; i++) {
            final ByteBuffer key = randomKeyBuffer();
            if (!topologyWithoutOne.primaryFor(key).equals(topologyFull.primaryFor(key))) {
                migration++;
            }
        }
        final int diff = Math.abs(numElementsForNodes - migration);
        assertTrue(diff < numElementsForNodes * DELTA);
    }
}
