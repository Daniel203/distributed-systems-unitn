package it.unitn.distributed;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import it.unitn.model.message.Messages.*;

public class Client extends AbstractActor {
    /**
     * Get latest stored value given a key
     * @param node Node to contact
     * @param key Key to search
     */
    public void get(ActorRef node, int key) {
        ClientGetRequestMsg req = new ClientGetRequestMsg(key);
        node.tell(req, getSelf());
    }

    /**
     * Update the value of a given key
     * @param node Node to contact
     * @param key Key to search
     * @param value Value to insert
     */
    public void update(ActorRef node, int key, String value) {
        ClientUpdateRequestMsg req = new ClientUpdateRequestMsg(key, value);
        node.tell(req, getSelf());
    }

    @Override
    public Receive createReceive() {
        return null;
    }
}
