package it.unitn.models;

import java.util.ArrayList;
import java.util.List;

import akka.actor.ActorRef;

public class WriteRequestContext {
    public final ActorRef client;
    public final int key;
    public final String newValue;
    public final List<StorageData> readReplies; 
    public int writeAcks; 

    public WriteRequestContext(ActorRef client, int key, String newValue) {
        this.client = client;
        this.key = key;
        this.newValue = newValue;
        this.readReplies = new ArrayList<StorageData>();
        this.writeAcks = 0;
    }
}
