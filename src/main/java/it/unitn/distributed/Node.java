package it.unitn.distributed;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.constraints.Constraints;
import it.unitn.constraints.Errors;
import it.unitn.dataStructures.CircularTreeMap;
import it.unitn.models.StorageData;
import it.unitn.models.WriteRequestContext;
import it.unitn.models.Messages.*;
import it.unitn.models.ReadRequestContext;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BinaryOperator;

public class Node extends AbstractActor {
    private final int id;
    private final CircularTreeMap<Integer, ActorRef> network;
    private final TreeMap<Integer, StorageData> storage;
    private final HashMap<UUID, ReadRequestContext> pendingReads;
    private final HashMap<UUID, WriteRequestContext> pendingWrites;
    private final HashMap<Integer, UUID> lockedKeys;
    private int pendingJoinReads;
    private boolean isRecovering;

    public Node(int id) {
        super();
        this.id = id;
        this.network = new CircularTreeMap<>();
        this.storage = new TreeMap<>();
        this.pendingReads = new HashMap<>();
        this.pendingWrites = new HashMap<>();
        this.lockedKeys = new HashMap<>();
        this.pendingJoinReads = 0;
        this.isRecovering = false;
    }

    public static Props props(int id) {
        return Props.create(Node.class, () -> new Node(id));
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                // Client request
                .match(ClientGetRequestMsg.class, this::onClientGetRequestMsg)
                .match(ClientUpdateRequestMsg.class, this::onClientUpdateRequestMsg)

                // Internal replica request
                .match(ReplicaReadRequestMsg.class, this::onReplicaReadRequestMsg)
                .match(ReplicaReadResponseMsg.class, this::onReplicaReadResponseMsg)
                .match(ReplicaWriteRequestMsg.class, this::onReplicaWriteRequestMsg)
                .match(ReplicaWriteResponseMsg.class, this::onReplicaWriteResponseMsg)
                .match(UnlockKeyMsg.class, this::onUnlockKeyMsg)

                // Network management
                .match(JoinMsg.class, this::onJoinMsg)
                .match(NodeListRequestMsg.class, this::onNodeListRequestMsg)
                .match(NodeListResponseMsg.class, this::onNodeListResponseMsg)
                .match(StorageRequestMsg.class, this::onStorageRequestMsg)
                .match(StorageResponseMsg.class, this::onStorageResponseMsg)
                .match(JoinedNetworkMsg.class, this::onJoinedNetworkMsg)
                .match(LeaveMsg.class, this::onLeaveMsg)
                .match(NodeLeavingMsg.class, this::onNodeLeavingMsg)
                .match(CrashMsg.class, this::onCrashMsg)
                .match(RecoverMsg.class, this::onRecoverMsg)

                // Debug and test
                .match(DebugPrintStateMsg.class, this::onDebugPrintStateMsg)
                .match(GetStateMsg.class, this::onGetStateMsg)
                .build();
    }

    public void onJoinMsg(JoinMsg msg) {
        network.put(this.id, getSelf());

        // If no bootstrapping peer, it means that the node is the first of the
        // network, hence no need to ask for other nodes in the network
        if (msg.bootstrapPeer() == null) {
            return;
        }

        // Ask the bootstrapping peer for the list of nodes in the network
        NodeListRequestMsg nodeListRequestMsg = new NodeListRequestMsg();
        msg.bootstrapPeer().tell(nodeListRequestMsg, getSelf());
    }

    public void onLeaveMsg(LeaveMsg msg) {
        // Calculate the ring without this node
        CircularTreeMap<Integer, ActorRef> nextRing = new CircularTreeMap<>();
        nextRing.putAll(this.network.getMap());
        nextRing.remove(this.id);

        // Map target nodes to the specific data they need to adopt
        // Key: Node ID, Value: The specific subset of data it should receive
        HashMap<Integer, TreeMap<Integer, StorageData>> handoffPackages = new HashMap<>();
        for (Map.Entry<Integer, StorageData> entry : this.storage.entrySet()) {
            int dataKey = entry.getKey();

            // Find the N responsible nodes in the new ring
            int currentNodeId = nextRing.nextKey(dataKey - 1);
            for (int i = 0; i < Constraints.N; i++) {
                handoffPackages.putIfAbsent(currentNodeId, new TreeMap<>());
                handoffPackages.get(currentNodeId).put(dataKey, entry.getValue());
                currentNodeId = nextRing.nextKey(currentNodeId);
            }
        }

        // Send the specific handoff package to each node that will receive data
        for (Map.Entry<Integer, TreeMap<Integer, StorageData>> entry : handoffPackages.entrySet()) {
            ActorRef targetNode = this.network.get(entry.getKey());
            NodeLeavingMsg nodeLeavingMsg = new NodeLeavingMsg(this.id, entry.getValue());
            targetNode.tell(nodeLeavingMsg, getSelf());
        }

        // Send empty map to the rest
        for (Map.Entry<Integer, ActorRef> entry : nextRing.entrySet()) {
            if (!handoffPackages.containsKey(entry.getKey())) {
                NodeLeavingMsg nodeLeavingMsg = new NodeLeavingMsg(this.id, new TreeMap<>());
                entry.getValue().tell(nodeLeavingMsg, getSelf());
            }
        }

        // Stop the node
        getContext().stop(getSelf());
    }

    public void onCrashMsg(CrashMsg msg) {
        // Just stop the node without any handoff
        getContext().stop(getSelf());
    }

    public void onRecoverMsg(RecoverMsg msg) {
        this.isRecovering = true;
        this.network.put(this.id, getSelf());

        // If no bootstrapping peer, it means that the node is the first of the
        // network, hence no need to ask for other nodes in the network
        if (msg.bootstrapPeer() == null) {
            this.isRecovering = false; // No need to recover data if I'm the first node
            return;
        }

        // Ask the peer for the network ring to start the data sync!
        NodeListRequestMsg nodeListRequestMsg = new NodeListRequestMsg();
        msg.bootstrapPeer().tell(nodeListRequestMsg, getSelf());
    }

    public void onNodeListRequestMsg(NodeListRequestMsg msg) {
        NodeListResponseMsg nodeListResponseMsg = new NodeListResponseMsg(this.network);
        getSender().tell(nodeListResponseMsg, getSelf());
    }

    public void onNodeListResponseMsg(NodeListResponseMsg msg) {
        // Add the response from the bootstrapping peer to the current network view
        // NOTE: putAll() because this.network contains self
        this.network.putAll(msg.network());

        // Get the nearest clockwise node
        ActorRef nextNodeValue = this.network.getNext(this.id);

        // Ask the nearest clockwise node for all the values that it holds
        StorageRequestMsg storageRequestMsg = new StorageRequestMsg();
        nextNodeValue.tell(storageRequestMsg, getSelf());

    }

    public void onStorageRequestMsg(StorageRequestMsg msg) {
        StorageResponseMsg storageResponseMsg = new StorageResponseMsg(this.storage);
        getSender().tell(storageResponseMsg, getSelf());
    }

    /**
     * When I receive the storage from the nearest clockwise node, I check which
     * data I am responsible for and trigger a read quorum for those data to get the
     * latest version safely. Once all the pending read quorums are completed, I can
     * finish the join phase by broadcasting that I joined the network (if it's a
     * normal join) or just quietly resuming operations (if it's a recovery).
     */
    public void onStorageResponseMsg(StorageResponseMsg msg) {
        this.pendingJoinReads = 0;

        for (Map.Entry<Integer, StorageData> entry : msg.storage().entrySet()) {
            int dataKey = entry.getKey();
            boolean amIResponsible = false;
            int currentNode = this.network.nextKey(dataKey - 1);

            for (int i = 0; i < Constraints.N; i++) {
                if (currentNode == this.id) {
                    amIResponsible = true;
                    break;
                }
                currentNode = this.network.nextKey(currentNode);
            }

            if (amIResponsible) {
                // Trigger a Quorum Read to get the latest version safely
                this.pendingJoinReads++;
                UUID requestId = UUID.randomUUID();

                // Pass getSelf() as a client so the read quorum replies to us
                ReadRequestContext context = new ReadRequestContext(getSelf(), dataKey, new ArrayList<>());
                pendingReads.put(requestId, context);

                int readNode = this.network.nextKey(dataKey - 1);
                for (int i = 0; i < Constraints.N; i++) {
                    var request = new ReplicaReadRequestMsg(requestId, dataKey, false);
                    this.network.get(readNode).tell(request, getSelf());
                    readNode = this.network.nextKey(readNode);
                }
            }
        }

        // If we didn't need to pull any data, finish immediately
        if (this.pendingJoinReads == 0) {
            finishJoinPhase();
        }
    }

    private void finishJoinPhase() {
        if (!this.isRecovering) {
            // Broadcast that I joined the network.
            for (Map.Entry<Integer, ActorRef> entry : this.network.entrySet()) {
                Integer key = entry.getKey();
                ActorRef node = entry.getValue();

                if (key != this.id) {
                    JoinedNetworkMsg joinedNetworkMsg = new JoinedNetworkMsg(this.id);
                    node.tell(joinedNetworkMsg, getSelf());
                }
            }
        } else {
            System.out.println("[NODE " + this.id + "] Recovery complete. Quietly resuming operations.");
            this.isRecovering = false;
        }
    }

    public void onJoinedNetworkMsg(JoinedNetworkMsg msg) {
        ActorRef joinedNode = getSender();
        this.network.put(msg.joinedNodeId(), joinedNode);

        // Iterate safely using an Iterator to avoid ConcurrentModificationException
        // while removing
        var iterator = this.storage.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Integer, StorageData> entry = iterator.next();

            boolean amIResponsible = false;
            int currentNode = this.network.nextKey(entry.getKey() - 1);

            // Check the N nodes responsible for this data
            for (int i = 0; i < Constraints.N; i++) {
                if (currentNode == this.id) {
                    amIResponsible = true;
                    break; // I am still one of the N responsible nodes
                }
                currentNode = this.network.nextKey(currentNode);
            }

            // If I am not responsible anymore, remove the data
            if (!amIResponsible) {
                iterator.remove();
            }
        }
    }

    public void onNodeLeavingMsg(NodeLeavingMsg msg) {
        this.network.remove(msg.leavingNodeId());

        // Check orphaned data to see if I am now responsible for it
        for (Map.Entry<Integer, StorageData> entry : msg.orphanedData().entrySet()) {
            int dataKey = entry.getKey();
            boolean amIResponsible = false;

            // Calculate the N nodes using the new smaller network view
            int currentNode = this.network.nextKey(dataKey - 1);
            for (int i = 0; i < Constraints.N; i++) {
                if (currentNode == this.id) {
                    amIResponsible = true;
                    break;
                }
                currentNode = this.network.nextKey(currentNode);
            }

            // If I am responsible, add the data to my storage
            if (amIResponsible) {
                this.storage.merge(dataKey, entry.getValue(),
                        BinaryOperator.maxBy(Comparator.comparingInt(StorageData::version)));
            }

        }
    }

    /**
     * This function is used by the client to ask the coordinator for the value of
     * a key. The coordinator will then contact the N nodes responsible for the key.
     */
    private void onClientGetRequestMsg(ClientGetRequestMsg msg) {
        UUID requestId = UUID.randomUUID();
        ReadRequestContext context = new ReadRequestContext(getSender(), msg.key(), new ArrayList<StorageData>());
        pendingReads.put(requestId, context);

        // Contact the N nodes
        int nextNode = this.network.nextKey(msg.key() - 1);
        for (int i = 0; i < Constraints.N; i++) {
            var request = new ReplicaReadRequestMsg(requestId, msg.key(), false);
            this.network.get(nextNode).tell(request, getSelf());

            nextNode = this.network.nextKey(nextNode);
        }
    }

    private void onClientUpdateRequestMsg(ClientUpdateRequestMsg msg) {
        UUID requestId = UUID.randomUUID();
        WriteRequestContext context = new WriteRequestContext(getSender(), msg.key(), msg.value());
        pendingWrites.put(requestId, context);

        // Contact the N nodes to find out the current version of the data
        int nextNode = this.network.nextKey(msg.key() - 1);
        for (int i = 0; i < Constraints.N; i++) {
            var request = new ReplicaReadRequestMsg(requestId, msg.key(), true);
            this.network.get(nextNode).tell(request, getSelf());
            nextNode = this.network.nextKey(nextNode);
        }
    }

    /**
     * This function is used by the coordinator to ask the replicas for the value of
     * a key. It replies with the value stored locally, if present, or null
     * otherwise.
     */
    private void onReplicaReadRequestMsg(ReplicaReadRequestMsg msg) {
        // Check if the key is locked by another write operation
        UUID currentLockOwner = lockedKeys.get(msg.key());
        if (currentLockOwner != null && !currentLockOwner.equals(msg.requestId())) {
            // It is locked. Deny the request to preserve sequential consistency
            ReplicaReadResponseMsg res = new ReplicaReadResponseMsg(msg.requestId(), msg.key(), null, true);
            getSender().tell(res, getSelf());
            return;
        }

        // If the coordinator intents to write, grant the lock
        if (msg.intentToWrite()) {
            lockedKeys.put(msg.key(), msg.requestId());

            // Schedule an auto-unlock slightly after the normal timeout
            // just in case the coordinator crashes before sending Phase 2
            getContext().getSystem().scheduler().scheduleOnce(
                    scala.concurrent.duration.Duration.create(Constraints.TIMEOUT + 500, TimeUnit.MILLISECONDS),
                    getSelf(),
                    new UnlockKeyMsg(msg.key(), msg.requestId()),
                    getContext().getSystem().dispatcher(),
                    ActorRef.noSender());
        }

        // Return the data
        StorageData value = this.storage.get(msg.key());
        ReplicaReadResponseMsg res = new ReplicaReadResponseMsg(msg.requestId(), msg.key(), value, false);
        getSender().tell(res, getSelf());
    }

    /**
     * This function is used to route incoming read responses from replicas to the
     * appropriate workflow. Because the same message is used for different
     * purposes, it checks the request ID to determine if the response belongs to an
     * ongoing client read quorum, the first phase of a client write operation
     */
    private void onReplicaReadResponseMsg(ReplicaReadResponseMsg msg) {
        if (pendingReads.containsKey(msg.requestId())) {
            handleClientReadQuorum(msg);
        } else if (pendingWrites.containsKey(msg.requestId())) {
            handleClientWritePhaseOne(msg);
        }
    }

    private void handleClientReadQuorum(ReplicaReadResponseMsg msg) {
        if (msg.lockDenied()) {
            ReadRequestContext context = pendingReads.remove(msg.requestId());
            // Don't send error messages to ourselves during a join
            if (context != null && !context.client.equals(getSelf())) {
                context.client.tell(new ClientGetResponseMsg(Errors.LOCK_DENIED_MSG), getSelf());
            }
            return;
        }

        ReadRequestContext context = pendingReads.get(msg.requestId());
        if (context == null) {
            return;
        }
        context.replies.add(msg.data());

        if (context.replies.size() == Constraints.R) {
            StorageData latestData = context.replies.stream()
                    .filter(data -> data != null)
                    .max(Comparator.comparingInt(StorageData::version))
                    .orElse(null);

            if (context.client.equals(getSelf())) { // Background join read
                if (latestData != null) {
                    this.storage.put(context.key, new StorageData(latestData.value(), latestData.version()));
                }
                this.pendingJoinReads--;

                // If all the backgrounds reads for the join phase are completed, we can finish
                // the join phase
                if (this.pendingJoinReads == 0) {
                    finishJoinPhase();
                }
            } else { // Normal client read
                String valueToReturn = (latestData != null) ? latestData.value() : Errors.KEY_NOT_FOUND_MSG;
                context.client.tell(new ClientGetResponseMsg(valueToReturn), getSelf());
            }

            pendingReads.remove(msg.requestId());
        }
    }

    private void handleClientWritePhaseOne(ReplicaReadResponseMsg msg) {
        if (msg.lockDenied()) {
            // If any replica denied the lock, we can immediately fail the write operation
            WriteRequestContext context = pendingWrites.remove(msg.requestId());
            if (context != null) {
                context.client.tell(new ClientUpdateResponseMsg(false), getSelf());

                // Unlock this key on all replicas that granted the lock
                int nextNode = this.network.nextKey(context.key - 1);
                for (int i = 0; i < Constraints.N; i++) {
                    var unlockMsg = new UnlockKeyMsg(context.key, msg.requestId());
                    this.network.get(nextNode).tell(unlockMsg, getSelf());
                    nextNode = this.network.nextKey(nextNode);
                }
            }

            return;
        }

        WriteRequestContext context = pendingWrites.get(msg.requestId());
        if (context == null) {
            return;
        }
        context.readReplies.add(msg.data());

        if (context.readReplies.size() == Constraints.W) {
            // Find the highest current version (if all are null, max is 0)
            int maxVersion = 0;
            for (StorageData d : context.readReplies) {
                if (d != null && d.version() > maxVersion) {
                    maxVersion = d.version();
                }
            }

            // Create the new data with an incremented version
            StorageData newData = new StorageData(context.newValue, maxVersion + 1);

            // Send write requests to the N replicas
            int nextNode = this.network.nextKey(context.key - 1);
            for (int i = 0; i < Constraints.N; i++) {
                var writeRequest = new ReplicaWriteRequestMsg(msg.requestId(), context.key, newData);
                this.network.get(nextNode).tell(writeRequest, getSelf());
                nextNode = this.network.nextKey(nextNode);
            }

            // Clean readRequest context to free memory
            context.readReplies.clear();
        }
    }

    private void onReplicaWriteRequestMsg(ReplicaWriteRequestMsg msg) {
        // Only the owner of the lock can unlock and write
        UUID lockOwner = lockedKeys.get(msg.key());
        if (lockOwner == null || lockOwner.equals(msg.requestId())) {
            lockedKeys.remove(msg.key());
            this.storage.merge(msg.key(), msg.data(),
                    BinaryOperator.maxBy(Comparator.comparingInt(StorageData::version)));
        }

        ReplicaWriteResponseMsg res = new ReplicaWriteResponseMsg(msg.requestId(), msg.key(), true);
        getSender().tell(res, getSelf());
    }

    private void onReplicaWriteResponseMsg(ReplicaWriteResponseMsg msg) {
        WriteRequestContext context = pendingWrites.get(msg.requestId());

        // If context is null, it means that the coordinator already replied to the
        // client
        if (context == null) {
            return;
        }

        if (msg.success()) {
            context.writeAcks++;
        }

        if (context.writeAcks == Constraints.W) {
            context.client.tell(new ClientUpdateResponseMsg(true), getSelf());
            pendingWrites.remove(msg.requestId());
        }
    }

    /**
     * This function is used by the coordinator to ask the replicas to release the
     * lock on a key after a write operation is completed. The replica checks if the
     * request ID matches the one that currently holds the lock on the key, and if
     * so, it releases the lock by removing the entry from the `lockedKeys` map.
     */
    public void onUnlockKeyMsg(UnlockKeyMsg msg) {
        UUID lockOwner = lockedKeys.get(msg.key());
        if (lockOwner != null && lockOwner.equals(msg.requestId())) {
            lockedKeys.remove(msg.key());
        }
    }

    private void onDebugPrintStateMsg(DebugPrintStateMsg msg) {
        System.out.println("=========================================");
        System.out.println("NODE " + this.id + " STATE:");

        // Print the network view
        System.out.print("  Network View: [");
        for (Integer nodeId : this.network.getMap().keySet()) {
            System.out.print(nodeId + " ");
        }
        System.out.println("]");

        // Print the stored data
        System.out.println("  Storage:");
        if (this.storage.isEmpty()) {
            System.out.println("    (empty)");
        } else {
            for (Map.Entry<Integer, StorageData> entry : this.storage.entrySet()) {
                System.out.println("    Key: " + entry.getKey() +
                        " | Value: '" + entry.getValue().value() +
                        "' | Version: " + entry.getValue().version());
            }
        }
        System.out.println("=========================================\n");
    }

    public void onGetStateMsg(GetStateMsg msg) {
        // Reply to the sender (the test runner) with a snapshot of our current state
        getSender().tell(
                new NodeStateReplyMsg(
                        new TreeMap<>(this.storage),
                        this.network.getMap()),
                getSelf());
    }
}
