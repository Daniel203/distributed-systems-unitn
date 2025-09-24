package it.unitn;

import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.InvalidActorNameException;
import it.unitn.messages.JoinMsg;
import it.unitn.node.Node;

import java.util.ArrayList;
import java.util.HashMap;

public class Manager {
    final static int R = 2;
    final static int W = 2;
    final static int N = 3;
    final static int TIMEOUT = 1000;  // in milliseconds

    private final HashMap<Integer, ActorRef> network = new HashMap<>();
    private final ActorSystem system;

    public Manager() {
        system = ActorSystem.create("MarsStorageSystem");
    }

    public void join(int nodeId) {
        try {
            ActorRef node = system.actorOf(Node.props(nodeId), String.valueOf(nodeId));

            // No check if it's already present because the initialization
            // returns an error if creating two nodes with the same id, so no need for
            // another check
            this.network.put(nodeId, node);

            // Select a node from the network as a bootstrapping peer for the join
            // If it's the first node of the network, select null
            ActorRef bootstrapPeer = this.network.values().stream().findFirst().orElse(null);

            // Tell the node to join the network
            JoinMsg joinMsg = new JoinMsg(bootstrapPeer);
            node.tell(joinMsg, Actor.noSender());
        } catch (InvalidActorNameException ex) {
            System.out.printf("Node with id %s cannot be created because it already exists.\n", nodeId);
        }
    }

    public void leave() {
    }

    public void crash() {
    }

    public void recovery() {
    }
}
