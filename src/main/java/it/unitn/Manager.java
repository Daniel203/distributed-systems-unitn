package it.unitn;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.node.Node;

import java.util.ArrayList;

public class Manager {
    final static int R = 2;
    final static int W = 2;
    final static int N = 3;
    final static int TIMEOUT = 1000;  // in milliseconds

    private final ArrayList<ActorRef> network = new ArrayList<>();
    private final ActorSystem system;

    public Manager() {
        system = ActorSystem.create("MarsStorageSystem");
    }



    private void join(String nodeId) {
        ActorRef node = system.actorOf(Node.props(), nodeId);
    }

    private void leave() {
    }

    private void crash() {
    }

    private void recovery() {
    }
}
