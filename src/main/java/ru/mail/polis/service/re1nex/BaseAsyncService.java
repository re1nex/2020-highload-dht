package ru.mail.polis.service.re1nex;

import one.nio.http.HttpClient;
import one.nio.net.ConnectionString;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.re1nex.Topology;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * BaseAsyncService provide service base methods of service (status, handleDefault)
 * and methods for asynchronous handling of requests (stop, executeTask - execute in ThreadPoolExecutor).
 */
abstract class BaseAsyncService extends BaseService {
    @NotNull
    protected final ApiController apiController;
    @NotNull
    protected final Map<String, HttpClient> nodeToClient;

    BaseAsyncService(final int port,
                     @NotNull final DAO dao,
                     final int workersCount,
                     final int queueSize,
                     @NotNull final Topology<String> topology,
                     @NotNull final Logger logger) throws IOException {
        super(port, dao, workersCount, queueSize, topology, logger);
        this.nodeToClient = new HashMap<>();
        for (final String node : topology.all()) {
            if (!topology.isLocal(node)) {
                final HttpClient client = new HttpClient(new ConnectionString(node + "?timeout=1000"));
                if (nodeToClient.put(node, client) != null) {
                    throw new IllegalStateException("Duplicate node");
                }
            }
        }
        this.apiController = new ApiController(dao, topology, nodeToClient, logger);
    }

    @Override
    public synchronized void stop() {
        super.stop();
        for (final HttpClient client : nodeToClient.values()) {
            client.clear();
        }
    }
}
