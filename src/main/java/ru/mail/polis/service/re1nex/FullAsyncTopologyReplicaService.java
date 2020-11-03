package ru.mail.polis.service.re1nex;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.re1nex.Topology;

import java.io.IOException;

/**
 * AsyncTopologyService provides asynchronous service with methods for work with shading systems with replicas
 * by asynchronous HTTPClient.
 */
public class FullAsyncTopologyReplicaService extends BaseService {
    @NotNull
    private static final Logger logger = LoggerFactory.getLogger(FullAsyncTopologyReplicaService.class);

    /**
     * Service for concurrent work with requests.
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
    }
}
