package ru.mail.polis.service.re1nex;

import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Response;
import one.nio.net.Socket;
import one.nio.server.RejectedSessionException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.re1nex.Topology;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * AsyncTopologyService provides asynchronous service with methods for work with shading systems with replicas
 * by asynchronous HTTPClient.
 */
public class FullAsyncTopologyReplicaService extends BaseService {
    @NotNull
    private static final Logger logger = LoggerFactory.getLogger(FullAsyncTopologyReplicaService.class);
    @NotNull
    private final DAO dao;

    /**
     * Asynchronous service for concurrent work with replicas for requests.
     *
     * @param port         - Server port
     * @param dao          - DAO impl
     * @param workersCount - number workers in pool
     * @param queueSize    - size of task's queue
     */
    public FullAsyncTopologyReplicaService(final int port,
                                           @NotNull final DAO dao,
                                           final int workersCount,
                                           final int queueSize,
                                           @NotNull final Topology<String> topology) throws IOException {
        super(port,
                workersCount,
                queueSize,
                logger,
                executors -> new AsyncApiControllerImpl(dao, topology, logger, executors),
                topology);
        this.dao = dao;
    }

    @Override
    public HttpSession createSession(@NotNull final Socket socket) {
        return new RangeStream(socket, this);
    }

    /**
     * Provide requests for stream of KV pairs from storage.
     *
     * @param start - start range
     * @param end   - end range(optional).
     */
    @Path("/v0/entities")
    public void getRange(
            @NotNull @Param(value = "start", required = true) final String start,
            @Nullable @Param(value = "end") final String end,
            @NotNull final HttpSession session
    ) {
        executeTask(() -> {
            if (start.isEmpty()) {
                ApiUtils.sendErrorResponse(session, Response.BAD_REQUEST, logger);
                return;
            }
            final ByteBuffer startByteBuffer = ByteBufferUtils.getByteBufferKey(start);
            final ByteBuffer endByteBuffer;
            if (end == null || end.isEmpty()) {
                endByteBuffer = null;
            } else {
                endByteBuffer = ByteBufferUtils.getByteBufferKey(end);
            }

            try {
                ((RangeStream) session).setIterator(dao.range(startByteBuffer, endByteBuffer));
            } catch (IOException e) {
                logger.error("Cannot handle range request: " + e.getMessage(), e);
                ApiUtils.sendErrorResponse(session, Response.INTERNAL_ERROR, logger);
            }

        }, session);
    }
}
