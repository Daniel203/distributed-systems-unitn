package it.unitn.distributed;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.constraints.Constraints;
import it.unitn.dataStructure.CircularTreeMap;
import it.unitn.model.StorageData;
import it.unitn.model.message.Messages.*;

import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BinaryOperator;

public class Node extends AbstractActor {
    private final int id;
    private final CircularTreeMap<Integer, ActorRef> network;
    private final TreeMap<Integer, StorageData> storage;

    public Node(int id) {
        super();
        this.id = id;
        this.network = new CircularTreeMap<>();
        this.storage = new TreeMap<>();
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
                .build();
    }

    private void onReplicaReadRequestMsg(ReplicaReadRequestMsg msg) {
        StorageData value = this.storage.get(msg.key());
        ReplicaReadResponseMsg res = new ReplicaReadResponseMsg(msg.key(), value);
        getSender().tell(res, getSelf());
    }

    /**
     * This function is used when a node joins the network and needs to
     * update all its values to the latest version. It calls the other nodes
     * with getRequestMsg, and it expects a response in order to compare its
     * data with the data of other nodes and keep the most up to date
     */
    private void onReplicaReadResponseMsg(ReplicaReadResponseMsg msg) {
        StorageData data = msg.data();

        if (data == null) {
            return;
        }

        this.storage.merge(msg.key(),
                data,
                BinaryOperator.maxBy(
                        Comparator.comparingInt(StorageData::version)));
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

                ClientGetRequestMsg getRequestMsg = new ClientGetRequestMsg(dataKey);
                this.network.get(currNode).tell(getRequestMsg, getSelf());

                // The check for the latest version is handled in onClientGetResponseMsg()
            }
        }

        // Broadcast that I joined the network.
        for (Map.Entry<Integer, ActorRef> entry : this.network.entrySet()) {
            Integer key = entry.getKey();
            ActorRef node = entry.getValue();

            if (key != this.id) {
                JoinedNetworkMsg joinedNetworkMsg = new JoinedNetworkMsg(this.id);
                node.tell(joinedNetworkMsg, getSelf());
            }
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

    private void onClientGetRequestMsg(ClientGetRequestMsg msg) {
        // TODO:  Read Quorum logic here 
    }

    private void onClientUpdateRequestMsg(ClientUpdateRequestMsg msg) {
        // TODO: Write Quorum logic here
    }

    private void onReplicaWriteRequestMsg(ReplicaWriteRequestMsg msg) {
        // TODO: Save the data and reply with success
    }

    private void onReplicaWriteResponseMsg(ReplicaWriteResponseMsg msg) {
        // TODO: Coordinator counts the successful writes
    }
}
