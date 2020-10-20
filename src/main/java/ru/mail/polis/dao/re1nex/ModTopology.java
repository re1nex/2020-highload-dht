package ru.mail.polis.dao.re1nex;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Set;

public class ModTopology implements Topology<String> {
    @NotNull
    private final String[] nodes;
    @NotNull
    private final String local;


    public ModTopology(@NotNull final Set<String> nodes, @NotNull final String local) {
        assert nodes.contains(local);

        this.local = local;
        this.nodes = new String[nodes.size()];
        nodes.toArray(this.nodes);
        Arrays.sort(this.nodes);
    }

    @Override
    public String primaryFor(@NotNull final ByteBuffer key) {
        return nodes[(key.hashCode() & Integer.MAX_VALUE) % nodes.length];
    }

    @Override
    public int size() {
        return nodes.length;
    }

    @NotNull
    @Override
    public String[] all() {
        return nodes.clone();
    }

    @Override
    public boolean isLocal(@NotNull final String node) {
        return local.equals(node);
    }
}
