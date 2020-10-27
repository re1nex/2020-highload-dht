package ru.mail.polis.service.re1nex;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import one.nio.http.HttpClient;
import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.ConnectionString;
import one.nio.server.AcceptorConfig;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.re1nex.Topology;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

abstract class BaseAsyncService extends HttpServer implements Service {
    @NotNull
    protected final ExecutorService executor;
    @NotNull
    protected final Map<String, HttpClient> nodeToClient;
    @NotNull
    protected final ApiController apiController;
    @NotNull
    private final Logger logger;

    BaseAsyncService(final int port,
                                @NotNull final DAO dao,
                                final int workersCount,
                                final int queueSize,
                                @NotNull final Topology<String> topology,
                                @NotNull final Logger logger) throws IOException {
        super(provideConfig(port));
        assert workersCount > 0;
        assert queueSize > 0;
        this.logger = logger;
        this.nodeToClient = new HashMap<>();
        this.apiController = new ApiController(dao, topology, nodeToClient, logger);
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
    }

    private static HttpServerConfig provideConfig(final int port) {
        final AcceptorConfig acceptorConfig = new AcceptorConfig();
        acceptorConfig.port = port;
        final HttpServerConfig httpServerConfig = new HttpServerConfig();
        httpServerConfig.acceptors = new AcceptorConfig[]{acceptorConfig};
        return httpServerConfig;
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
    public void handleDefault(final Request request,
                              final HttpSession session) throws IOException {
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

    protected void executeTask(final Runnable task, final HttpSession session) {
        try {
            executor.execute(task);
        } catch (RejectedExecutionException e) {
            logger.error("Execute failed!", e);
            apiController.sendErrorResponse(session, Response.SERVICE_UNAVAILABLE);
        }
    }
}
