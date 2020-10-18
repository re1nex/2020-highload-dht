package ru.mail.polis.dao.re1nex;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public interface Topology<N> {
    N primaryFor(@NotNull final ByteBuffer key);

    int size();

    @NotNull
    N[] all();

    boolean isLocal(@NotNull final N node);
}
