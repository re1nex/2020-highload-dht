package ru.mail.polis.service.re1nex;

import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.RequestMethod;
import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.re1nex.Topology;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.NoSuchElementException;

/**
 * AsyncTopologyService provides asynchronous service with methods for work with shading systems.
 */
public class AsyncTopologyService extends BaseAsyncService {
    @NotNull
    private static final Logger logger = LoggerFactory.getLogger(AsyncTopologyService.class);
    @NotNull
    private final DAO dao;
    @NotNull
    private final Topology<String> topology;

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
        super(port, dao, workersCount, queueSize, topology, logger);
        this.dao = dao;
        this.topology = topology;
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
                        ApiUtils.sendErrorResponse(session, Response.BAD_REQUEST, logger);
                        return;
                    }
                    final ByteBuffer key = ByteBufferUtils.getByteBufferKey(id);
                    final String node;
                    try {
                        node = topology.primaryFor(key);
                    } catch (NoSuchAlgorithmException e) {
                        logger.error("Get failed! Can`t use hash ", e);
                        ApiUtils.sendErrorResponse(session, Response.INTERNAL_ERROR, logger);
                        return;
                    }
                    if (topology.isLocal(node)) {
                        getFromNode(session, key, id);
                    } else {
                        ApiUtils.proxy(node,
                                request,
                                session,
                                nodeToClient.get(node), logger);
                    }
                },
                session);
    }

    private void getFromNode(@NotNull final HttpSession session,
                             @NotNull final ByteBuffer key,
                             @NotNull final String id) {
        try {
            final byte[] result = ByteBufferUtils.byteBufferToByte(dao.get(key));
            if (result.length > 0) {
                ApiUtils.sendResponse(session, new Response(Response.OK, result), logger);
            } else {
                ApiUtils.sendResponse(session, new Response(Response.OK, Response.EMPTY), logger);
            }
        } catch (IOException e) {
            logger.error("GET element " + id, e);
            ApiUtils.sendErrorResponse(session, Response.INTERNAL_ERROR, logger);
        } catch (NoSuchElementException exception) {
            logger.info("GET failed! no element " + id, exception);
            ApiUtils.sendErrorResponse(session, Response.NOT_FOUND, logger);
        }
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
                        ApiUtils.sendErrorResponse(session, Response.BAD_REQUEST, logger);
                        return;
                    }
                    final ByteBuffer key = ByteBufferUtils.getByteBufferKey(id);
                    final String node;
                    try {
                        node = topology.primaryFor(key);
                    } catch (NoSuchAlgorithmException e) {
                        logger.error("Put failed! Can`t use hash ", e);
                        ApiUtils.sendErrorResponse(session, Response.INTERNAL_ERROR, logger);
                        return;
                    }
                    if (topology.isLocal(node)) {
                        try {
                            dao.upsert(key,
                                    ByteBuffer.wrap(request.getBody()));
                            ApiUtils.sendResponse(session, new Response(Response.CREATED, Response.EMPTY), logger);
                        } catch (IOException e) {
                            logger.error("PUT failed! Cannot put the element: {}. Request: {}. Cause: {}",
                                    id, request.getBody(), e.getCause());
                            ApiUtils.sendErrorResponse(session, Response.INTERNAL_ERROR, logger);
                        }
                    } else {
                        ApiUtils.proxy(node,
                                request,
                                session,
                                nodeToClient.get(node), logger);
                    }
                },
                session);
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
                        ApiUtils.sendErrorResponse(session, Response.BAD_REQUEST, logger);
                        return;
                    }
                    final ByteBuffer key = ByteBufferUtils.getByteBufferKey(id);
                    final String node;
                    try {
                        node = topology.primaryFor(key);
                    } catch (NoSuchAlgorithmException e) {
                        logger.error("Delete failed! Can`t use hash ", e);
                        ApiUtils.sendErrorResponse(session, Response.INTERNAL_ERROR, logger);
                        return;
                    }
                    if (topology.isLocal(node)) {
                        try {
                            dao.remove(ByteBufferUtils.getByteBufferKey(id));
                            ApiUtils.sendResponse(session, new Response(Response.ACCEPTED, Response.EMPTY), logger);
                        } catch (IOException e) {
                            logger.error("DELETE failed! Cannot get the element {}.\n Error: {}",
                                    id, e.getMessage(), e);
                            ApiUtils.sendErrorResponse(session, Response.INTERNAL_ERROR, logger);
                        }
                    } else {
                        ApiUtils.proxy(node,
                                request,
                                session,
                                nodeToClient.get(node), logger);
                    }
                },
                session);
    }
}
