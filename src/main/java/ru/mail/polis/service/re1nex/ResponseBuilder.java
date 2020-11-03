package ru.mail.polis.service.re1nex;

import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Objects;

final class ResponseBuilder {
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
        this.value = value;
        this.resultCode = resultCode;
        this.onlyStatus = false;
    }

    public int getStatus() {
        String s = resultCode;
        return (s.charAt(0) * 100) + (s.charAt(1) * 10) + s.charAt(2) - ('0' * 111);
    }

    ResponseBuilder(@NotNull final String resultCode,
                    final long generation,
                    @Nullable final byte[] value) {
        this(resultCode, generation, value == null, value);
    }

    ResponseBuilder(@NotNull final String resultCode,
                    final long generation,
                    final boolean isTombstone) {
        this(resultCode, generation, isTombstone, null);
    }

    ResponseBuilder(@NotNull final String resultCode) {
        this(resultCode, 0, false, null);
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
}
