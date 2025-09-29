package it.unitn.distributed;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.constraints.Constraints;
import it.unitn.dataStructure.CircularTreeMap;
import it.unitn.model.message.*;
import it.unitn.model.StorageData;

import java.util.Map;
import java.util.TreeMap;

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
                .match(GetRequestMsg.class, this::onGetRequestMsg)
                .match(UpdateRequestMsg.class, this::onWriteRequestMsg)
                .match(JoinMsg.class, this::onJoinMsg)
                .match(NodeListRequestMsg.class, this::onNodeListRequestMsg)
                .match(NodeListResponseMsg.class, this::onNodeListResponseMsg)
                .match(JoinedNetworkMsg.class, this::onJoinedNetworkMsg)
                .match(StorageRequestMsg.class, this::onStorageRequestMsg)
                .match(StorageResponseMsg.class, this::onStorageResponseMsg)
                .build();
    }

    private void onGetRequestMsg(GetRequestMsg msg) {
        // TODO: get the actual value
        GetResponseMsg res = new GetResponseMsg(1);
        getSender().tell(res, getSelf());
    }

    private void onWriteRequestMsg(UpdateRequestMsg msg) {
        // update(key)
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
        // NOTE: addAll() because this.network contains self
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
        // Where id(first node holding data, then rotate clockwise replicaCount times) >= idNode
        // idData = 10, idNode = 20, replicaCount = 3, nodes = 5, 15, 20, 30
        // 30 >= 20 -> keep data
        for (Map.Entry<Integer, StorageData> entry : msg.storage().entrySet()) {
            var nextNthNodeKey = this.network.nthNextKey(entry.getKey(), Constraints.N);
            if (nextNthNodeKey >= this.id) {
                this.storage.put(entry.getKey(), entry.getValue());
            }
        }

        // TODO: Check that the values are up to date (call other nodes with those values)

        // Broadcast that I joined the network.
        for (Map.Entry<Integer, ActorRef> entry : this.network.entrySet()) {
            Integer key = entry.getKey();
            ActorRef node = entry.getValue();

            if (key != this.id) {
                JoinedNetworkMsg joinedNetworkMsg = new JoinedNetworkMsg();
                node.tell(joinedNetworkMsg, getSelf());
            }
        }
    }

    public void onJoinedNetworkMsg(JoinedNetworkMsg msg) {
        ActorRef joinedNode = getSender();

        /*
        if (joinedNode == null || this.network.contains(joinedNode)) {
            return;
        }

        this.network.add(joinedNode);
         */
    }
}
