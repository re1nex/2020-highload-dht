package ru.mail.polis.service.re1nex;

import one.nio.http.HttpClient;
import one.nio.net.ConnectionString;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.re1nex.Topology;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * AsyncTopologyService provides asynchronous service with methods for work with shading systems with replicas.
 */
public class AsyncTopologyReplicaService extends BaseService {
    @NotNull
    private static final Logger logger = LoggerFactory.getLogger(AsyncTopologyReplicaService.class);
    @NotNull
    protected final Map<String, HttpClient> nodeToClient;

    /**
     * Service for concurrent work with replicas for requests.
     *
     * @param port         - Server port
     * @param dao          - DAO impl
     * @param workersCount - number workers in pool
     * @param queueSize    - size of task's queue
     */
    public AsyncTopologyReplicaService(final int port,
                                       @NotNull final DAO dao,
                                       final int workersCount,
                                       final int queueSize,
                                       @NotNull final Topology<String> topology,
                                       @NotNull final Map<String, HttpClient> nodeToClient) throws IOException {
        super(port,
                workersCount,
                queueSize,
                logger,
                executors -> new ApiControllerImpl(dao, topology, nodeToClient, logger),
                topology);
        this.nodeToClient = nodeToClient;
    }

    @Override
    public synchronized void stop() {
        super.stop();
        for (final HttpClient client : nodeToClient.values()) {
            client.clear();
        }
    }

    /*
    Create nodeToClient for AsyncTopologyReplicaService and AsyncTopologyReplicaService.

     * @param port         - Server port
     * @param dao          - DAO impl
     * @param workersCount - number workers in pool
     * @param queueSize    - size of task's queue
     * @return a AsyncTopologyReplicaService instance
     */
    public static BaseService createService(final int port,
                                   @NotNull final DAO dao,
                                   final int workersCount,
                                   final int queueSize,
                                   @NotNull final Topology<String> topology) throws IOException {
        final Map<String, HttpClient> nodeToClient = new HashMap<>();
        for (final String node : topology.all()) {
            if (!topology.isLocal(node)) {
                final HttpClient client = new HttpClient(new ConnectionString(node + "?timeout=1000"));
                if (nodeToClient.put(node, client) != null) {
                    throw new IllegalStateException("Duplicate node");
                }
            }
        }
        return new AsyncTopologyReplicaService(port,
                dao,
                workersCount,
                queueSize,
                topology,
                nodeToClient);
    }
}
