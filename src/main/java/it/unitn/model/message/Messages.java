package it.unitn.model.message;

import java.util.TreeMap;
import akka.actor.ActorRef;
import it.unitn.dataStructure.CircularTreeMap;
import it.unitn.model.StorageData;

public interface Messages {

    // Network management messages (join phase)
    public record JoinMsg(ActorRef bootstrapPeer) {}
    public record NodeListRequestMsg() {}
    public record NodeListResponseMsg(CircularTreeMap<Integer, ActorRef> network) {}
    public record StorageRequestMsg() {}
    public record StorageResponseMsg(TreeMap<Integer, StorageData> storage) {}
    public record JoinedNetworkMsg(int joinedNodeId) {}

    // Sent by the Client to any Node
    public record ClientGetRequestMsg(int key) {}
    public record ClientUpdateRequestMsg(int key, String value) {}
    
    // Sent by the Coordinator back to the Client
    public record ClientGetResponseMsg(int key, StorageData data) {}
    public record ClientUpdateResponseMsg(boolean success) {}

    // Sent by the Coordinator to the N replicas responsible for the key
    public record ReplicaReadRequestMsg(int key) {}
    public record ReplicaWriteRequestMsg(int key, StorageData data) {}

    // Sent by the replicas back to the Coordinator
    public record ReplicaReadResponseMsg(int key, StorageData data) {}
    public record ReplicaWriteResponseMsg(int key, boolean success) {}
}
