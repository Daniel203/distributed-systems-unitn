package it.unitn.models;

import akka.actor.ActorRef;
import it.unitn.dataStructures.CircularTreeMap;

import java.util.HashMap;
import java.util.Random;
import java.util.TreeMap;
import java.util.UUID;

public class NodeContext {
    public final int id;
    public final CircularTreeMap<Integer, ActorRef> network;
    public final TreeMap<Integer, StorageData> storage;
    public final HashMap<UUID, ReadRequestContext> pendingReads;
    public final HashMap<UUID, WriteRequestContext> pendingWrites;
    public final HashMap<Integer, UUID> lockedKeys;
    public int pendingJoinReads;
    public boolean isRecovering;
    public final Random random;

    public NodeContext(int id) {
        this.id = id;
        this.network = new CircularTreeMap<>();
        this.storage = new TreeMap<>();
        this.pendingReads = new HashMap<>();
        this.pendingWrites = new HashMap<>();
        this.lockedKeys = new HashMap<>();
        this.pendingJoinReads = 0;
        this.isRecovering = false;
        this.random = new Random();
    }
}
