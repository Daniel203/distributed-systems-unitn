package it.unitn.models;

import java.util.TreeMap;
import java.util.UUID;

import akka.actor.ActorRef;
import it.unitn.dataStructures.CircularTreeMap;

public interface Messages {

    // Network management messages 
    public record JoinMsg(ActorRef bootstrapPeer) {}
    public record NodeListRequestMsg() {}
    public record NodeListResponseMsg(CircularTreeMap<Integer, ActorRef> network) {}
    public record StorageRequestMsg() {}
    public record StorageResponseMsg(TreeMap<Integer, StorageData> storage) {}
    public record JoinedNetworkMsg(int joinedNodeId) {}
    public record LeaveMsg() {}
    public record NodeLeavingMsg(int leavingNodeId, TreeMap<Integer, StorageData> orphanedData) {}
    public record CrashMsg() {}
    public record RecoverMsg(ActorRef bootstrapPeer) {}

    // Timeout tracking
    public record CheckTimeoutMsg(UUID requestId) {}

    // Sent by the Client to any Node
    public record ClientGetRequestMsg(int key) {}
    public record ClientUpdateRequestMsg(int key, String value) {}
    
    // Sent by the Coordinator back to the Client
    public record ClientGetResponseMsg(String data) {}
    public record ClientUpdateResponseMsg(boolean success) {}

    // Sent by the Coordinator to the N replicas responsible for the key
    // `intentToWrite` flag to indicate if the read is part of a write operation, so
    // that the replica can decide whether to grant a lock or not.
    public record ReplicaReadRequestMsg(UUID requestId, int key, boolean intentToWrite) {}
    public record ReplicaWriteRequestMsg(UUID requestId, int key, StorageData data) {}
    public record UnlockKeyMsg(int key, UUID requestId) {}
    // Sent by the replicas back to the Coordinator
    public record ReplicaReadResponseMsg(UUID requestId, int key, StorageData data, boolean lockDenied) {}
    public record ReplicaWriteResponseMsg(UUID requestId, int key, boolean success) {}


    // Debug and test
    public record DebugPrintStateMsg() {}
    public record GetStateMsg() {}
    public record NodeStateReplyMsg(java.util.TreeMap<Integer, StorageData> storage, 
                                    java.util.Map<Integer, ActorRef> networkView) {}
}
