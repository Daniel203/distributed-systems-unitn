package it.unitn.models;

import java.util.List;
import akka.actor.ActorRef;

public class ReadRequestContext {
    public final ActorRef client;
    public final int key;
    public final List<StorageData> replies;

    public ReadRequestContext(ActorRef client, int key, List<StorageData> replies) {
        this.client = client;
		this.key = key;
        this.replies = replies;
    }

}
