package ru.mail.polis.service.re1nex;

import one.nio.http.HttpSession;
import one.nio.http.Request;
import org.jetbrains.annotations.NotNull;

public interface ApiController {
    void sendReplica(@NotNull final String id,
                     @NotNull final ReplicaInfo replicaInfo,
                     @NotNull final HttpSession session,
                     @NotNull final Request request);

    void handleResponseLocal(@NotNull final String id,
                             @NotNull final HttpSession session,
                             @NotNull final Request request);
}
