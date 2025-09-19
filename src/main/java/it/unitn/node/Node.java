package it.unitn.node;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.messages.GetRequestMsg;
import it.unitn.messages.GetResponseMsg;
import it.unitn.messages.UpdateRequestMsg;

import java.util.HashMap;
import java.util.List;

public class Node extends AbstractActor {
    private final int id;
    private List<ActorRef> network;
    private final HashMap<Integer, StorageData> storage;

    public Node(int id) {
        super();
        this.id = id;
        this.storage = new HashMap<>();
    }

    public static Props props() {
        return Props.create(Node.class);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(GetRequestMsg.class, this::onGetRequestMsg)
                .match(UpdateRequestMsg.class, this::onWriteRequestMsg)
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
}
