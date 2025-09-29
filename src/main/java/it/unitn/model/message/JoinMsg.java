package it.unitn.model.message;

import akka.actor.ActorRef;

public record JoinMsg(ActorRef bootstrapPeer) { }
