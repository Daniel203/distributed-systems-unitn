package it.unitn.node;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.messages.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Node extends AbstractActor {
    private final int id;
    private final HashMap<Integer, ActorRef> network;
    private final HashMap<Integer, StorageData> storage;

    public Node(int id) {
        super();
        this.id = id;
        this.network = new HashMap<>();
        this.storage = new HashMap<>();
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
        if (msg.network().isEmpty()) {
            return;
        }

        // Add the response from the bootstrapping peer to the current network view
        // NOTE: addAll() because this.network contains self
        this.network.putAll(msg.network());

        // TODO: ask clockwise nearest node for all the data in storage that the current node is responsible for
        // Find the nearest clockwise node
        int nearestNodeId;
        for (Map.Entry<Integer, ActorRef> entry : msg.network().entrySet()) {
            // TODO: consider that the network is circular
        }

        // Broadcast that I joined the network.
        // NOTE: foreach on msg.network to avoid including self
        for (ActorRef node : msg.network().values()) {
            JoinedNetworkMsg joinedNetworkMsg = new JoinedNetworkMsg();
            node.tell(joinedNetworkMsg, getSelf());
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
