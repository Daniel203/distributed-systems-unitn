package it.unitn.models;

import java.util.List;
import akka.actor.ActorRef;

public class ReadRequestContext {
    public final ActorRef client;
    public final List<StorageData> replies;

    public ReadRequestContext(ActorRef client, List<StorageData> replies) {
        this.client = client;
        this.replies = replies;
    }

}
