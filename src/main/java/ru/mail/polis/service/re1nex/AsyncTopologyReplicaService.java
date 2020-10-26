package ru.mail.polis.service.re1nex;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import one.nio.http.HttpClient;
import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.ConnectionString;
import one.nio.server.AcceptorConfig;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.re1nex.ReplicaInfo;
import ru.mail.polis.dao.re1nex.Topology;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class AsyncTopologyReplicaService extends HttpServer implements Service {
    @NotNull
    private static final Logger logger = LoggerFactory.getLogger(AsyncTopologyReplicaService.class);
    @NotNull
    private final ExecutorService executor;
    @NotNull
    private final Map<String, HttpClient> nodeToClient;
    @NotNull
    private final ApiUtils apiUtils;
    @NotNull
    private final ReplicaInfo defaultReplicaInfo;

    /**
     * Service for concurrent work with requests.
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
                                       @NotNull final Topology<String> topology) throws IOException {
        super(provideConfig(port));
        assert workersCount > 0;
        assert queueSize > 0;
        this.nodeToClient = new HashMap<>();
        final int from = topology.getUniqueSize();
        final int ack = from / 2 + 1;
        this.defaultReplicaInfo = new ReplicaInfo(ack, from);
        for (final String node : topology.all()) {
            if (!topology.isLocal(node)) {
                final HttpClient client = new HttpClient(new ConnectionString(node + "?timeout=1000"));
                if (nodeToClient.put(node, client) != null) {
                    throw new IllegalStateException("Duplicate node");
                }
            }
        }
        this.executor = new ThreadPoolExecutor(
                workersCount,
                workersCount,
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(queueSize),
                new ThreadFactoryBuilder()
                        .setUncaughtExceptionHandler((t, e) -> logger.error("Error {} in thread {}", e, t))
                        .setNameFormat("worker_%d")
                        .build(),
                new ThreadPoolExecutor.AbortPolicy()
        );
        this.apiUtils = new ApiUtils(dao, topology, nodeToClient, logger);
    }

    private static HttpServerConfig provideConfig(final int port) {
        final AcceptorConfig acceptorConfig = new AcceptorConfig();
        acceptorConfig.port = port;
        final HttpServerConfig httpServerConfig = new HttpServerConfig();
        httpServerConfig.acceptors = new AcceptorConfig[]{acceptorConfig};
        return httpServerConfig;
    }

    /**
     * Provide requests for AsyncTopologyReplicaService
     */
    @Path("/v0/entity")
    public void handleRequest(@Param(value = "id", required = true) final String id,
                              @Param("replicas") final String replicas,
                              @NotNull final HttpSession session,
                              @NotNull final Request request) {
        executor.execute(() -> {
            if (id.isEmpty()) {
                logger.info("Id is empty!");
                apiUtils.sendErrorResponse(session, Response.BAD_REQUEST);
                return;
            }
            if (request.getHeader(ApiUtils.PROXY) == null) {
                final ReplicaInfo replicaInfo;
                if (replicas != null) {
                    try {
                        replicaInfo = ReplicaInfo.of(replicas);
                    } catch (IllegalArgumentException exception) {
                        logger.info("Wrong params replica", exception);
                        apiUtils.sendResponse(session, new Response(Response.BAD_REQUEST, Response.EMPTY));
                        return;
                    }
                } else {
                    replicaInfo = defaultReplicaInfo;
                }
                apiUtils.sendReplica(id, replicaInfo, session, request);
            } else {
                apiUtils.sendProxy(id,
                        session,
                        request);
            }
        });
    }

    /**
     * Provide service status.
     *
     * @return Response - status
     */
    @Path("/v0/status")
    public Response status() {
        return Response.ok(Response.OK);
    }

    @Override
    public void handleDefault(final Request request, final HttpSession session) throws IOException {
        logger.info("Unsupported mapping request.\n Cannot understand it: {} {}",
                request.getMethodName(), request.getPath());
        session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
    }

    @Override
    public synchronized void stop() {
        super.stop();
        executor.shutdown();
        try {
            executor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error("Can't shutdown execution");
            Thread.currentThread().interrupt();
        }
        for (final HttpClient client : nodeToClient.values()) {
            client.clear();
        }
    }
}
