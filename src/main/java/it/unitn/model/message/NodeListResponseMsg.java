package it.unitn.model.message;

import akka.actor.ActorRef;
import it.unitn.dataStructure.CircularTreeMap;

public record NodeListResponseMsg(CircularTreeMap<Integer, ActorRef> network) {
}
