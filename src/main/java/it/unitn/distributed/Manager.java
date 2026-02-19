package it.unitn.distributed;

import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.InvalidActorNameException;
import it.unitn.model.message.Messages.*;

import java.util.TreeMap;


public class Manager {
    private final TreeMap<Integer, ActorRef> network = new TreeMap<>();
    private final ActorSystem system;

    public Manager() {
        system = ActorSystem.create("MarsStorageSystem");
    }

    public void join(int nodeId) {
        try {
            ActorRef node = system.actorOf(Node.props(nodeId), String.valueOf(nodeId));

            // Doesn't check if it's already present because the initialization
            // returns an error if creating two nodes with the same id
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

    public ActorRef getNodeById(int id) {
        return this.network.get(id);
    }

    public ActorSystem getSystem() {
        return this.system;
    }
}
