package it.unitn.messages;

import akka.actor.ActorRef;

import java.util.HashMap;
import java.util.List;

public record NodeListResponseMsg(HashMap<Integer, ActorRef> network) {
}
