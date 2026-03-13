package it.unitn.models;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import akka.actor.ActorRef;
import it.unitn.dataStructures.CircularTreeMap;
import it.unitn.models.Messages.ClientUpdateRequestMsg;

/**
 * All mutable state for a node
 * 
 * Static persistentDisks = hard drive (survives crashes).
 * Everything else = RAM (lost when the actor is stopped).
 */
public class NodeContext {

    public final int id;

    // Static: lives outside any actor lifecycle, survives crashes.
    private static final ConcurrentHashMap<Integer, TreeMap<Integer, StorageData>> persistentDisks = new ConcurrentHashMap<>();

    public final CircularTreeMap<Integer, ActorRef> network;
    public final TreeMap<Integer, StorageData> storage;

    // In-flight reads and writes, keyed by UUID.
    public final HashMap<UUID, ReadRequestContext> pendingReads;
    public final HashMap<UUID, WriteRequestContext> pendingWrites;

    // Writes queued behind an in-flight write for the same key (same coordinator).
    // Prevents a symmetric lock deadlock when two writes for the same key start
    // simultaneously.
    public final HashMap<Integer, ArrayDeque<Map.Entry<ClientUpdateRequestMsg, ActorRef>>> waitingWrites = new HashMap<>();

    // Keys locked by an active write on this node acting as replica: key ->
    // ownerUUID.
    public final HashMap<Integer, UUID> lockedKeys;

    // Counts background quorum reads during join/recovery.
    public int pendingJoinReads;

    // Suppresses the JoinedNetworkMsg broadcast at the end of recovery.
    public boolean isRecovering;

    public final Random random;

    public NodeContext(int id) {
        this.id = id;
        this.network = new CircularTreeMap<>();
        persistentDisks.putIfAbsent(id, new TreeMap<>());
        this.storage = persistentDisks.get(id);
        this.pendingReads = new HashMap<>();
        this.pendingWrites = new HashMap<>();
        this.lockedKeys = new HashMap<>();
        this.pendingJoinReads = 0;
        this.isRecovering = false;
        this.random = new Random();
    }

    /** Wipes all simulated disks. Called by the test harness before each test. */
    public static void formatAllDisks() {
        persistentDisks.clear();
    }
}
