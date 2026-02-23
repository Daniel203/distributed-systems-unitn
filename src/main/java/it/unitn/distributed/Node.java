package it.unitn.distributed;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.constraints.Constraints;
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
import java.util.function.BinaryOperator;

public class Node extends AbstractActor {
    private final int id;
    private final CircularTreeMap<Integer, ActorRef> network;
    private final TreeMap<Integer, StorageData> storage;
    private final HashMap<UUID, ReadRequestContext> pendingReads;
    private final HashMap<UUID, WriteRequestContext> pendingWrites;
    private boolean isRecovering;

    public Node(int id) {
        super();
        this.id = id;
        this.network = new CircularTreeMap<>();
        this.storage = new TreeMap<>();
        this.pendingReads = new HashMap<>();
        this.pendingWrites = new HashMap<>();
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
        // Broadcast that I am leaving the network
        for (Map.Entry<Integer, ActorRef> entry : this.network.entrySet()) {
            Integer key = entry.getKey();
            ActorRef node = entry.getValue();

            if (key != this.id) {
                NodeLeavingMsg nodeLeavingMsg = new NodeLeavingMsg(this.id, this.storage);
                node.tell(nodeLeavingMsg, getSelf());
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

    public void onStorageResponseMsg(StorageResponseMsg msg) {
        // Keep only the data that the current node is responsible for
        for (Map.Entry<Integer, StorageData> entry : msg.storage().entrySet()) {
            boolean amIResponsible = false;

            // Start exactly at the key or the first node after
            int currentNode = this.network.nextKey(entry.getKey() - 1);

            // Walk clockwise up to N times
            for (int i = 0; i < Constraints.N; i++) {
                if (currentNode == this.id) {
                    amIResponsible = true;
                    break; // I am one of the N responsible nodes
                }
                currentNode = this.network.nextKey(currentNode);
            }

            // If my ID was found in the N responsible nodes, keep the data
            if (amIResponsible) {
                this.storage.put(entry.getKey(), entry.getValue());
            }
        }

        // Call other nodes having the same data and keep the latest version
        for (int dataKey : this.storage.keySet()) {
            int currNode = this.id;

            // Call the next N nodes to be sure to retrieve all the replicas of the data
            for (int i = 0; i < Constraints.N; i++) {
                currNode = this.network.nextKey(currNode);

                ReplicaReadRequestMsg getRequestMsg = new ReplicaReadRequestMsg(UUID.randomUUID(), dataKey);
                this.network.get(currNode).tell(getRequestMsg, getSelf());
            }
        }

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
        ReadRequestContext context = new ReadRequestContext(getSender(), new ArrayList<StorageData>());
        pendingReads.put(requestId, context);

        // Contact the N nodes
        int nextNode = this.network.nextKey(msg.key() - 1);
        for (int i = 0; i < Constraints.N; i++) {
            var request = new ReplicaReadRequestMsg(requestId, msg.key());
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
            var request = new ReplicaReadRequestMsg(requestId, msg.key());
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
        StorageData value = this.storage.get(msg.key());
        ReplicaReadResponseMsg res = new ReplicaReadResponseMsg(msg.requestId(), msg.key(), value);
        getSender().tell(res, getSelf());
    }

    /**
     * This function is used to route incoming read responses from replicas to the
     * appropriate workflow. Because the same message is used for different
     * purposes, it checks the request ID to determine if the response belongs to an
     * ongoing client read quorum, the first phase of a client write operation, or a
     * background data synchronization during a node's join phase.
     */
    private void onReplicaReadResponseMsg(ReplicaReadResponseMsg msg) {
        if (pendingReads.containsKey(msg.requestId())) {
            handleClientReadQuorum(msg);
        } else if (pendingWrites.containsKey(msg.requestId())) {
            handleClientWritePhaseOne(msg);
        } else {
            handleJoinBackgroundSync(msg);
        }
    }

    private void handleClientReadQuorum(ReplicaReadResponseMsg msg) {
        ReadRequestContext context = pendingReads.get(msg.requestId());
        context.replies.add(msg.data());

        if (context.replies.size() >= Constraints.R) {
            StorageData latestData = context.replies.stream()
                    .filter(data -> data != null)
                    .max(Comparator.comparingInt(StorageData::version))
                    .orElse(null);

            String valueToReturn = (latestData != null) ? latestData.value() : Constraints.KEY_NOT_FOUND_MSG;
            context.client.tell(new ClientGetResponseMsg(valueToReturn), getSelf());
            pendingReads.remove(msg.requestId());
        }
    }

    private void handleClientWritePhaseOne(ReplicaReadResponseMsg msg) {
        WriteRequestContext context = pendingWrites.get(msg.requestId());
        context.readReplies.add(msg.data());

        if (context.readReplies.size() >= Constraints.W) {
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

    private void handleJoinBackgroundSync(ReplicaReadResponseMsg msg) {
        StorageData data = msg.data();
        if (data == null)
            return;

        this.storage.merge(msg.key(), data,
                BinaryOperator.maxBy(Comparator.comparingInt(StorageData::version)));
    }

    private void onReplicaWriteRequestMsg(ReplicaWriteRequestMsg msg) {
        this.storage.merge(msg.key(), msg.data(),
                BinaryOperator.maxBy(Comparator.comparingInt(StorageData::version)));
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

        if (context.writeAcks >= Constraints.W) {
            context.client.tell(new ClientUpdateResponseMsg(true), getSelf());
            pendingWrites.remove(msg.requestId());
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
}
