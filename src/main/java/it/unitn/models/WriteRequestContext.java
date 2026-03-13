package it.unitn.models;

import java.util.ArrayList;
import java.util.List;

import akka.actor.ActorRef;

/**
 * State for one in-flight write operation (coordinator side).
 *
 * Phase 1: readReplies accumulates lock-grants until W arrived.
 * Phase 2: writeAcks counts commit acks until W arrived.
 * phase2Started: late lockDenied replies after phase 2 begins are ignored.
 */
public class WriteRequestContext {
    public final ActorRef client;
    public final int key;
    public final String newValue;
    public final List<StorageData> readReplies; 
    public int writeAcks; 
    public boolean phase2Started;

    public WriteRequestContext(ActorRef client, int key, String newValue) {
        this.client = client;
        this.key = key;
        this.newValue = newValue;
        this.readReplies = new ArrayList<StorageData>();
        this.writeAcks = 0;
        this.phase2Started = false;
    }
}
