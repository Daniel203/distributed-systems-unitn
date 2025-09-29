package it.unitn.distributed;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import it.unitn.model.message.GetRequestMsg;
import it.unitn.model.message.UpdateRequestMsg;

public class Client extends AbstractActor {
    public void get(ActorRef node, int key) {
        GetRequestMsg req = new GetRequestMsg(key);
        node.tell(req, getSelf());
    }

    public void update(ActorRef node, int key, int value) {
        UpdateRequestMsg req = new UpdateRequestMsg(key, value);
        node.tell(req, getSelf());
    }

    @Override
    public Receive createReceive() {
        return null;
    }
}
