package ru.mail.polis.dao.re1nex;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.Set;

public interface Topology<N> {
    N primaryFor(@NotNull final ByteBuffer key) throws NoSuchAlgorithmException;

    int size();

    @NotNull
    N[] all();

    @NotNull
    Set<N> severalNodesForKey(@NotNull final ByteBuffer key, final int numNodes) throws NoSuchAlgorithmException;

    boolean isLocal(@NotNull final N node);

    boolean removeLocal(@NotNull final Set<N> nodes);

    int getUniqueSize();
}
