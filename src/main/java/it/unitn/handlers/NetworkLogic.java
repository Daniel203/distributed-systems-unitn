package it.unitn.handlers;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.BinaryOperator;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import it.unitn.constraints.Constraints;
import it.unitn.dataStructures.CircularTreeMap;
import it.unitn.models.Messages.CrashMsg;
import it.unitn.models.Messages.FinishJoinPhaseMsg;
import it.unitn.models.Messages.JoinMsg;
import it.unitn.models.Messages.JoinedNetworkMsg;
import it.unitn.models.Messages.LeaveMsg;
import it.unitn.models.Messages.NodeLeavingMsg;
import it.unitn.models.Messages.NodeListRequestMsg;
import it.unitn.models.Messages.NodeListResponseMsg;
import it.unitn.models.Messages.RecoverMsg;
import it.unitn.models.Messages.ReplicaReadRequestMsg;
import it.unitn.models.Messages.StorageRequestMsg;
import it.unitn.models.Messages.StorageResponseMsg;
import it.unitn.models.NodeContext;
import it.unitn.models.ReadRequestContext;
import it.unitn.models.StorageData;

/**
 * Handles ring topology changes: join, leave, crash, recovery.
 */
public class NetworkLogic extends BaseLogic {

    public NetworkLogic(NodeContext ctx, ActorContext actorContext, ActorRef self) {
        super(ctx, actorContext, self);
    }


    // ---- Join ---------------------------------------------------------------

    /** Step 1: register self, then request the node list from the bootstrap peer. */
    public void onJoinMsg(JoinMsg msg) {
        ctx.network.put(ctx.id, self);

        if (msg.bootstrapPeer() != null) {
            NodeListRequestMsg nodeListRequestMsg = new NodeListRequestMsg();
            sendWithDelay(msg.bootstrapPeer(), nodeListRequestMsg, self);
        }
    }

    public void onNodeListRequestMsg(NodeListRequestMsg msg, ActorRef sender) {
        NodeListResponseMsg nodeListResponseMsg = new NodeListResponseMsg(ctx.network);
        sendWithDelay(sender, nodeListResponseMsg, self);
    }

    /** Step 2: populate network view, then ask the clockwise neighbour for its storage. */
    public void onNodeListResponseMsg(NodeListResponseMsg msg) {
        ctx.network.putAll(msg.network());
        ctx.network.put(ctx.id, self);  // overwrite any stale reference the peer may have add

        ActorRef nextNodeValue = ctx.network.getNext(ctx.id);
        sendWithDelay(nextNodeValue, new StorageRequestMsg(), self);
    }

    public void onStorageRequestMsg(StorageRequestMsg msg, ActorRef sender) {
        StorageResponseMsg storageResponseMsg = new StorageResponseMsg(ctx.storage);
        sendWithDelay(sender, storageResponseMsg, self);
    }

    /**
     * Step 3: for every key we are responsible for, issue a background quorum
     * read (client=self, skipping self as a source since our copy may be stale).
     * Keys we are no longer responsible for are deleted from disk.
     * When all reads finish, FinishJoinPhaseMsg is sent to ensure it is processed
     * after any read replies still in flight.
     */
    public void onStorageResponseMsg(StorageResponseMsg msg) {
        ctx.pendingJoinReads = 0;

        // Combine keys from the neighbor's storage and our own persistent disk
        Set<Integer> keysToCheck = new HashSet<>();
        keysToCheck.addAll(msg.storage().keySet());
        keysToCheck.addAll(ctx.storage.keySet());

        for (int dataKey : keysToCheck) {
            if (ctx.network.isResponsible(ctx.id, dataKey, Constraints.N)) {
                ctx.pendingJoinReads++;
                UUID requestId = UUID.randomUUID();

                ReadRequestContext context = new ReadRequestContext(self, dataKey, new ArrayList<>());
                ctx.pendingReads.put(requestId, context);

                List<Integer> responsibleNodes = ctx.network.getNResponsibleNodes(dataKey, Constraints.N);
                for (int readNodeId : responsibleNodes) {
                    if (readNodeId != ctx.id) {  // skip self; our copy may be stale
                        var request = new ReplicaReadRequestMsg(requestId, dataKey, false);
                        sendWithDelay(ctx.network.get(readNodeId), request, self);
                    }
                }
            } else {
                ctx.storage.remove(dataKey);  // no longe responsible (ring changed during recovery)
            }
        }

        if (ctx.pendingJoinReads == 0) {
            self.tell(new FinishJoinPhaseMsg(), self);
        }
    }

    /**
     * All background reads are done.
     * Join: broadcast JoinedNetworkMsg so peers update their views.
     * Recovery: quietly resume (peers never removed us from their views).
     */
    public void onFinishJoinPhaseMsg(FinishJoinPhaseMsg msg) {
        if (!ctx.isRecovering) {
            for (Map.Entry<Integer, ActorRef> entry : ctx.network.entrySet()) {
                if (entry.getKey() != ctx.id) {
                    JoinedNetworkMsg joinedNetworkMsg = new JoinedNetworkMsg(ctx.id);
                    sendWithDelay(entry.getValue(), joinedNetworkMsg, self);
                }
            }
        } else {
            System.out.println("[NODE " + ctx.id + "] Recovery complete. Quietly resuming operations.");
            ctx.isRecovering = false;
        }
    }

    /** Adds the new node to our view and drops keys we are no longer responsible for. */
    public void onJoinedNetworkMsg(JoinedNetworkMsg msg, ActorRef sender) {
        ctx.network.put(msg.joinedNodeId(), sender);
        ctx.storage.entrySet().removeIf(e -> !ctx.network.isResponsible(ctx.id, e.getKey(), Constraints.N));
    }


    // ---- Leave --------------------------------------------------------------

    /**
     * Sends data only to nodes that are newly responsible in the future ring
     * (not already holding a replica). All other nodes receive an empty payload
     * so they still learn about the departure.
     */
    public void onLeaveMsg(LeaveMsg msg) {
        CircularTreeMap<Integer, ActorRef> nextRing = ctx.network.cloneWithout(ctx.id);
        HashMap<Integer, TreeMap<Integer, StorageData>> handoffPackages = new HashMap<>();

        for (Map.Entry<Integer, StorageData> entry : ctx.storage.entrySet()) {
            int dataKey = entry.getKey();

            List<Integer> newlyResponsible = nextRing.getNResponsibleNodes(dataKey, Constraints.N);
            List<Integer> currentlyResponsible = ctx.network.getNResponsibleNodes(dataKey, Constraints.N);
            currentlyResponsible.remove((Integer) ctx.id);

            for (int newNodeId : newlyResponsible) {
                if (!currentlyResponsible.contains(newNodeId)) {  // doesn't already hold a replica
                    handoffPackages.putIfAbsent(newNodeId, new TreeMap<>());
                    handoffPackages.get(newNodeId).put(dataKey, entry.getValue());
                }
            }
        }

        // Send data handoff packages to nodes that need new data
        for (Map.Entry<Integer, TreeMap<Integer, StorageData>> entry : handoffPackages.entrySet()) {
            ActorRef targetNode = ctx.network.get(entry.getKey());
            NodeLeavingMsg nodeLeavingMsg = new NodeLeavingMsg(ctx.id, entry.getValue());
            sendWithDelay(targetNode, nodeLeavingMsg, self);
        }

        // Notify all remaining nodes of the departure (with empty data if no handoff needed)
        for (Map.Entry<Integer, ActorRef> entry : nextRing.entrySet()) {
            if (!handoffPackages.containsKey(entry.getKey())) {
                NodeLeavingMsg nodeLeavingMsg = new NodeLeavingMsg(ctx.id, new TreeMap<>());
                sendWithDelay(entry.getValue(), nodeLeavingMsg, self);
            }
        }

        actorContext.stop(self);
    }

    /** Removes the leaving node from our view and merges any handed-off data. */
    public void onNodeLeavingMsg(NodeLeavingMsg msg) {
        ctx.network.remove(msg.leavingNodeId());

        for (Map.Entry<Integer, StorageData> entry : msg.orphanedData().entrySet()) {
            int dataKey = entry.getKey();

            if (ctx.network.isResponsible(ctx.id, dataKey, Constraints.N)) {
                ctx.storage.merge(dataKey, entry.getValue(),
                        BinaryOperator.maxBy(Comparator.comparingInt(StorageData::version)));
            }
        }
    }

     
    // ---- Crash & Recovery ---------------------------------------------------

    /** Stops the actor, clearing all RAM. The persistentDisks entry survives. */
    public void onCrashMsg(CrashMsg msg) {
        actorContext.stop(self);
    }

    /**
     * New actor, same ID — NodeContext re-attaches to the existing disk entry.
     * Recovery follows the same sync path as join via onStorageResponseMsg,
     * but skips the JoinedNetworkMsg broadcast (peers never removed this node).
     */
    public void onRecoverMsg(RecoverMsg msg) {
        ctx.isRecovering = true;
        ctx.network.put(ctx.id, self);

        if (msg.bootstrapPeer() == null) {
            ctx.isRecovering = false;
            return;
        }

        NodeListRequestMsg nodeListRequestMsg = new NodeListRequestMsg();
        sendWithDelay(msg.bootstrapPeer(), nodeListRequestMsg, self);
    }

}
