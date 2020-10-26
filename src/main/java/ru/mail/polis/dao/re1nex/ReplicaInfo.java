package ru.mail.polis.dao.re1nex;

import com.google.common.base.Splitter;
import org.jetbrains.annotations.NotNull;

import java.util.List;

public class ReplicaInfo {
    private final int ack;
    private final int from;

    public ReplicaInfo(final int ack, final int from) {
        this.ack = ack;
        this.from = from;
    }

    public int getAck() {
        return ack;
    }

    public int getFrom() {
        return from;
    }

    @NotNull
    public static ReplicaInfo of(@NotNull final String replica) {
        List<String> values = Splitter.on('/').splitToList(replica);
        if (values.size() != 2) {
            throw new IllegalArgumentException("Not enough args");
        }
        final int ack = Integer.parseInt(values.get(0));
        final int from = Integer.parseInt(values.get(1));
        if (ack > from || ack <= 0) {
            throw new IllegalArgumentException("Wrong args");
        }
        return new ReplicaInfo(ack, from);
    }
}
