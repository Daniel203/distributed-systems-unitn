package it.unitn.models;

import java.util.List;

import akka.actor.ActorRef;

/**
 * State for one in-flight read operation (coordinator side).
 * When client == self, this is a background join/recovery read:
 * the result is merged into local storage instead of forwarded.
 */
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
