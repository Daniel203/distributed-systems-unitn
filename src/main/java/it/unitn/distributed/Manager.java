package it.unitn.distributed;

import java.util.TreeMap;

import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.InvalidActorNameException;
import it.unitn.models.Messages.CrashMsg;
import it.unitn.models.Messages.JoinMsg;
import it.unitn.models.Messages.LeaveMsg;
import it.unitn.models.Messages.RecoverMsg;

 
/**
 * System administrator. Lives outside the ring; creates and destroys node actors.
 * Maintains its own live-node map so management commands can be routed by ID.
 */
public class Manager {
    private final TreeMap<Integer, ActorRef> network = new TreeMap<>();
    private final ActorSystem system;

    public Manager() {
        system = ActorSystem.create("MarsStorageSystem");
    }

    public void join(int nodeId) {
        /** Spawns a new node and tells it to join the ring via a bootstrap peer. */
        try {
            ActorRef node = system.actorOf(Node.props(nodeId), String.valueOf(nodeId));

            // Doesn't check if it's already present because the initialization
            // returns an error if creating two nodes with the same id
            this.network.put(nodeId, node);

            // Select a node from the network as a bootstrapping peer for the join
            // If it's the first node of the network, select null
            ActorRef bootstrapPeer = this.network.values().stream()
                    .filter(n -> !n.equals(node))
                    .findFirst()
                    .orElse(null);


            // Tell the node to join the network
            JoinMsg joinMsg = new JoinMsg(bootstrapPeer);
            node.tell(joinMsg, Actor.noSender());
        } catch (InvalidActorNameException ex) {
            System.out.printf("Node with id %s cannot be created because it already exists.\n", nodeId);
        }
    }

    /** Tells the node to leave gracefully (hands off data, then stops itself). */
    public void leave(int nodeId) {
        var node = this.network.get(nodeId);

        if (node == null) {
            System.out.printf("Node with id %s cannot leave because it doesn't exist.\n", nodeId);
            return;
        }

        node.tell(new LeaveMsg(), Actor.noSender());
        this.network.remove(nodeId);
    }

    /** Simulates a power failure: stops the actor immediately with no data handoff. */
    public void crash(int nodeId) {
        ActorRef node = this.network.get(nodeId);
        if (node != null) {
            // Tell the node to die immediately
            node.tell(new CrashMsg(), Actor.noSender());

            // Remove it from the manager's active map so we know it's offline.
            this.network.remove(nodeId);
        } else {
            System.out.printf("Node with id %s cannot crash because it does not exist.\n", nodeId);
        }
    }

    /** Spawns a replacement actor for a crashed node; it re-attaches to the existing disk entry. */
    public void recover(int nodeId) {
        try {
            ActorRef node = system.actorOf(Node.props(nodeId), String.valueOf(nodeId));
            this.network.put(nodeId, node);

            // Peek a survivor node to use as bootstrap peer for the join
            ActorRef bootstrapPeer = this.network.values().stream()
                    .filter(n -> !n.equals(node))
                    .findFirst()
                    .orElse(null);

            node.tell(new RecoverMsg(bootstrapPeer), Actor.noSender());
        } catch (InvalidActorNameException ex) {
            System.out.println("Node " + nodeId + " is already running. Cannot recover.");
        }

    }

    public ActorRef getNodeById(int id) {
        return this.network.get(id);
    }

    public ActorSystem getSystem() {
        return this.system;
    }
}
