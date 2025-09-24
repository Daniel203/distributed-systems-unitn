package it.unitn.messages;

import akka.actor.ActorRef;

public record JoinMsg(ActorRef bootstrapPeer) { }
