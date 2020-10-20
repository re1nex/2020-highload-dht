package ru.mail.polis.service.re1nex;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import one.nio.http.HttpClient;
import one.nio.http.HttpException;
import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.RequestMethod;
import one.nio.http.Response;
import one.nio.net.ConnectionString;
import one.nio.pool.PoolException;
import one.nio.server.AcceptorConfig;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.re1nex.Topology;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class AsyncTopologyService extends HttpServer implements Service {
    @NonNull
    private static final String RESPONSE_ERROR = "Can't send response error";
    @NotNull
    private static final Logger logger = LoggerFactory.getLogger(AsyncTopologyService.class);
    @NotNull
    private final DAO dao;
    @NotNull
    private final ExecutorService executor;
    @NotNull
    private final Topology<String> topology;
    @NotNull
    private final Map<String, HttpClient> nodeToClient;

    /**
     * Service for concurrent work with requests.
     *
     * @param port         - Server port
     * @param dao          - DAO impl
     * @param workersCount - number workers in pool
     * @param queueSize    - size of task's queue
     */
    public AsyncTopologyService(final int port,
                                @NotNull final DAO dao,
                                final int workersCount,
                                final int queueSize,
                                @NotNull final Topology<String> topology) throws IOException {
        super(provideConfig(port));
        assert workersCount > 0;
        assert queueSize > 0;
        this.dao = dao;
        this.topology = topology;
        this.nodeToClient = new HashMap<>();
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
    public void handleDefault(final Request request, final HttpSession session) throws IOException {
        logger.info("Unsupported mapping request.\n Cannot understand it: {} {}",
                request.getMethodName(), request.getPath());
        session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
    }

    /**
     * Provide request to get the value by id.
     * send 200 OK ||  400 / 404 / 500 ERROR
     *
     * @param id      - key
     * @param session - current HttpSession
     */
    @Path("/v0/entity")
    @RequestMethod(Request.METHOD_GET)
    public void get(@Param(value = "id", required = true) final String id,
                    @NotNull final Request request,
                    @NotNull final HttpSession session) {
        executeTask(() -> {
                    if (id.isEmpty()) {
                        logger.info("GET failed! Id is empty!");
                        sendErrorResponse(session, Response.BAD_REQUEST);
                        return;
                    }
                    final ByteBuffer key = getByteBufferKey(id);
                    final String node = topology.primaryFor(key);
                    if (topology.isLocal(node)) {
                        try {
                            final byte[] result = ByteBufferToByte(dao.get(key));
                            sendResponse(session, new Response(Response.OK,
                                    Objects.requireNonNullElse(result, Response.EMPTY)));
                        } catch (IOException e) {
                            logger.error("GET element " + id, e);
                            sendErrorResponse(session, Response.INTERNAL_ERROR);
                        } catch (NoSuchElementException exception) {
                            logger.info("GET failed! no element " + id, exception);
                            sendErrorResponse(session, Response.NOT_FOUND);
                        }
                    } else {
                        proxy(node, request, session);
                    }
                },
                session);
    }

    @Nullable
    private byte[] ByteBufferToByte(@NotNull final ByteBuffer result) {
        if (result.hasRemaining()) {
            final byte[] resultByteArray = new byte[result.remaining()];
            result.get(resultByteArray);
            return resultByteArray;
        } else {
            return null;
        }
    }

    private void sendResponse(@NotNull final HttpSession session, @NotNull final Response response) {
        try {
            session.sendResponse(response);
        } catch (IOException e) {
            logger.error("Cannot send respose", e);
        }
    }

    private void proxy(
            @NotNull final String node,
            @NotNull final Request request,
            @NotNull final HttpSession session) {
        try {
            request.addHeader("X-Proxy-For: " + node);
            sendResponse(session, nodeToClient.get(node).invoke(request));
        } catch (IOException | InterruptedException | PoolException | HttpException e) {
            logger.error(RESPONSE_ERROR, e);
            sendErrorResponse(session, Response.INTERNAL_ERROR);
        }
    }

    @NotNull
    private ByteBuffer getByteBufferKey(@NotNull final String id) {
        return ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Provide request to put the value by id.
     * send 201 Created || 400 / 500 ERROR
     *
     * @param id      - key
     * @param request - Request with value
     * @param session - current HttpSession
     */
    @Path("/v0/entity")
    @RequestMethod(Request.METHOD_PUT)
    public void put(@Param(value = "id", required = true) final String id,
                    @NotNull final Request request,
                    @NotNull final HttpSession session) {
        executeTask(() -> {
                    if (id.isEmpty()) {
                        logger.info("PUT failed! Id is empty!");
                        sendErrorResponse(session, Response.BAD_REQUEST);
                        return;
                    }
                    final ByteBuffer key = getByteBufferKey(id);
                    final String node = topology.primaryFor(key);
                    if (topology.isLocal(node)) {
                        try {
                            dao.upsert(key,
                                    ByteBuffer.wrap(request.getBody()));
                            sendResponse(session, new Response(Response.CREATED, Response.EMPTY));
                        } catch (IOException e) {
                            logger.error("PUT failed! Cannot put the element: {}. Request: {}. Cause: {}",
                                    id, request.getBody(), e.getCause());
                            sendErrorResponse(session, Response.INTERNAL_ERROR);
                        }
                    } else {
                        proxy(node, request, session);
                    }
                },
                session);
    }

    private void sendErrorResponse(final HttpSession session, final String internalError) {
        try {
            session.sendResponse(new Response(internalError, Response.EMPTY));
        } catch (IOException ioException) {
            logger.error(RESPONSE_ERROR, ioException);
        }
    }

    /**
     * Provide request to delete the value by id.
     * send 202 Accepted ||  400 / 500 ERROR
     *
     * @param id      - key
     * @param session - current HttpSession
     */
    @Path("/v0/entity")
    @RequestMethod(Request.METHOD_DELETE)
    public void delete(@Param(value = "id", required = true) final String id,
                       @NotNull final Request request,
                       @NotNull final HttpSession session) {
        executeTask(() -> {
                    if (id.isEmpty()) {
                        logger.info("DELETE failed! Id is empty!");
                        sendErrorResponse(session, Response.BAD_REQUEST);
                        return;
                    }
                    final ByteBuffer key = getByteBufferKey(id);
                    final String node = topology.primaryFor(key);
                    if (topology.isLocal(node)) {
                        try {
                            dao.remove(getByteBufferKey(id));
                            sendResponse(session, new Response(Response.ACCEPTED, Response.EMPTY));
                        } catch (IOException e) {
                            logger.error("DELETE failed! Cannot get the element {}.\n Error: {}",
                                    id, e.getMessage(), e);
                            sendErrorResponse(session, Response.INTERNAL_ERROR);
                        }
                    } else {
                        proxy(node, request, session);
                    }
                },
                session);
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

    private void executeTask(final Runnable task, final HttpSession session) {
        try {
            executor.execute(task);
        } catch (RejectedExecutionException e) {
            logger.error("Execute failed!", e);
            sendErrorResponse(session, Response.SERVICE_UNAVAILABLE);
        }
    }
}
