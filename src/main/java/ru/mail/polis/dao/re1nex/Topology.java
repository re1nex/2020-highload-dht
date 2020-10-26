package ru.mail.polis.dao.re1nex;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;

public interface Topology<N> {
    N primaryFor(@NotNull final ByteBuffer key) throws NoSuchAlgorithmException;

    int size();

    @NotNull
    N[] all();

    boolean isLocal(@NotNull final N node);
}
