/*
 * Copyright 2020 (c) Odnoklassniki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ru.mail.polis.service;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Unit tests for read-repair of a three node replicated {@link Service} cluster.
 */
class ReadRepairTest extends ClusterTestBase {
    private static final Duration TIMEOUT = Duration.ofMinutes(1);

    @Override
    int getClusterSize() {
        return 3;
    }

    @Test
    void putTest() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            final String key = randomId();

            for (int node = 0; node < getClusterSize(); node++) {
                // Reinitialize cluster
                restartAllNodes();

                // Stop node
                stop(node);

                // Insert value
                final byte[] value = randomValue();
                assertEquals(201, upsert((node + 1) % getClusterSize(), key, value, 2, 3).getStatus());

                // Start node
                createAndStart(node);

                // Check
                checkResponse(200, value, get(node, key, 2, 3));

                //stop all that wasn't missed
                for (int i = 0; i < getClusterSize(); i++) {
                    if (i != node) {
                        stop(i);
                    }
                }

                checkResponse(200, value, get(node, key, 1, 3));

                // Help implementors with ms precision for conflict resolution
                waitForVersionAdvancement();
            }
        });
    }

    @Test
    void deleteTest() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            final String key = randomId();

            for (int node = 0; node < getClusterSize(); node++) {
                // Reinitialize cluster
                restartAllNodes();

                // Insert value
                final byte[] value = randomValue();
                assertEquals(201, upsert((node + 1) % getClusterSize(), key, value, 3, 3).getStatus());

                // Help implementors with ms precision for conflict resolution
                waitForVersionAdvancement();

                // Stop node
                stop(node);

                // Delete
                assertEquals(202, delete((node + 1) % getClusterSize(), key, 2, 3).getStatus());

                // Start node
                createAndStart(node);

                // Check
                assertEquals(404, get(node, key, 3, 3).getStatus());

                //stop all that wasn't missed
                for (int i = 0; i < getClusterSize(); i++) {
                    if (i != node) {
                        stop(i);
                    }
                }

                assertEquals(404, get(node, key, 1, 3).getStatus());

                // Help implementors with ms precision for conflict resolution
                waitForVersionAdvancement();
            }
        });
    }

    @Test
    void updateTest() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            final String key = randomId();

            for (int node = 0; node < getClusterSize(); node++) {
                // Reinitialize cluster
                restartAllNodes();

                // Insert value
                final byte[] value = randomValue();
                assertEquals(201, upsert((node + 1) % getClusterSize(), key, value, 3, 3).getStatus());

                // Check
                checkResponse(200, value, get(node, key, 3, 3));

                // Stop node
                stop(node);

                // update value
                final byte[] value2 = randomValue();
                assertEquals(201, upsert((node + 1) % getClusterSize(), key, value2, 2, 3).getStatus());

                // Start node
                createAndStart(node);

                // Check
                checkResponse(200, value2, get(node, key, 2, 3));

                //stop all that wasn't missed
                for (int i = 0; i < getClusterSize(); i++) {
                    if (i != node) {
                        stop(i);
                    }
                }


                checkResponse(200, value2, get(node, key, 1, 3));

                // Help implementors with ms precision for conflict resolution
                waitForVersionAdvancement();
            }
        });
    }
}
