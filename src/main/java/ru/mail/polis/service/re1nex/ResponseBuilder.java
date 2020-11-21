package ru.mail.polis.service.re1nex;

import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Objects;

final class ResponseBuilder {
    @Nullable
    private String node;
    private final boolean isTombstone;
    private final long generation;
    @Nullable
    private final byte[] value;
    @NotNull
    private final String resultCode;
    private boolean onlyStatus;

    ResponseBuilder(@NotNull final String resultCode,
                    final long generation,
                    final boolean isTombstone,
                    @Nullable final byte[] value) {
        this.isTombstone = isTombstone;
        this.generation = generation;
        this.value = value == null ? null : value.clone();
        this.resultCode = resultCode;
        this.onlyStatus = false;
    }

    public int getStatusCode() {
        final String s = resultCode;
        return (s.charAt(0) * 100) + (s.charAt(1) * 10) + s.charAt(2) - ('0' * 111);
    }

    ResponseBuilder(@NotNull final String resultCode,
                    final long generation,
                    @Nullable final byte[] value,
                    @NotNull final String node) {
        this(resultCode, generation, value == null, value);
        this.node = node;
    }

    ResponseBuilder(@NotNull final String resultCode,
                    @NotNull final String node,
                    final long generation,
                    final boolean isTombstone) {
        this(resultCode, generation, isTombstone, null);
        this.node = node;
    }

    ResponseBuilder(@NotNull final String resultCode) {
        this(resultCode, 0, false, null);
        this.onlyStatus = true;
    }

    ResponseBuilder(@NotNull final String resultCode, @NotNull final String node) {
        this(resultCode, 0, false, null);
        this.node = node;
        this.onlyStatus = true;
    }

    @NotNull
    Response getResponse() {
        final Response response;
        if (onlyStatus) {
            return new Response(resultCode, Response.EMPTY);
        } else if (isTombstone) {
            response = new Response(Response.OK, Response.EMPTY);
            response.addHeader(ApiUtils.TOMBSTONE + ": True");
        } else {
            response = new Response(Response.OK, Objects.requireNonNull(value));
        }
        if (node != null) {
            response.addHeader(ApiUtils.FROM_NODE + ": " + node);
        }
        response.addHeader(ApiUtils.GENERATION + ": " + generation);
        return response;
    }

    public boolean isTombstone() {
        return isTombstone;
    }

    public long getGeneration() {
        return generation;
    }

    @Nullable
    public byte[] getValue() {
        return value == null ? null : value.clone();
    }

    @Nullable
    public String getNode() {
        return node;
    }
}
