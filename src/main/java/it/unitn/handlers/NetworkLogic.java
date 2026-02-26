package it.unitn.handlers;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import it.unitn.constraints.Constraints;
import it.unitn.dataStructures.CircularTreeMap;
import it.unitn.models.NodeContext;
import it.unitn.models.ReadRequestContext;
import it.unitn.models.StorageData;
import it.unitn.models.Messages.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.BinaryOperator;
import java.util.Comparator;

public class NetworkLogic extends BaseLogic {

    public NetworkLogic(NodeContext ctx, ActorContext actorContext, ActorRef self) {
        super(ctx, actorContext, self);
    }

    public void onJoinMsg(JoinMsg msg) {
        ctx.network.put(ctx.id, self);

        if (msg.bootstrapPeer() == null) {
            return;
        }

        NodeListRequestMsg nodeListRequestMsg = new NodeListRequestMsg();
        sendWithDelay(msg.bootstrapPeer(), nodeListRequestMsg, self);
    }

    public void onNodeListRequestMsg(NodeListRequestMsg msg, ActorRef sender) {
        NodeListResponseMsg nodeListResponseMsg = new NodeListResponseMsg(ctx.network);
        sendWithDelay(sender, nodeListResponseMsg, self);
    }

    public void onNodeListResponseMsg(NodeListResponseMsg msg) {
        ctx.network.putAll(msg.network());
        ActorRef nextNodeValue = ctx.network.getNext(ctx.id);
        sendWithDelay(nextNodeValue, new StorageRequestMsg(), self);
    }

    public void onStorageRequestMsg(StorageRequestMsg msg, ActorRef sender) {
        StorageResponseMsg storageResponseMsg = new StorageResponseMsg(ctx.storage);
        sendWithDelay(sender, storageResponseMsg, self);
    }

    public void onStorageResponseMsg(StorageResponseMsg msg) {
        ctx.pendingJoinReads = 0;

        for (Map.Entry<Integer, StorageData> entry : msg.storage().entrySet()) {
            int dataKey = entry.getKey();

            if (ctx.network.isResponsible(ctx.id, dataKey, Constraints.N)) {
                ctx.pendingJoinReads++;
                UUID requestId = UUID.randomUUID();

                ReadRequestContext context = new ReadRequestContext(self, dataKey, new ArrayList<>());
                ctx.pendingReads.put(requestId, context);

                for (int readNodeId : ctx.network.getNResponsibleNodes(dataKey, Constraints.N)) {
                    var request = new ReplicaReadRequestMsg(requestId, dataKey, false);
                    sendWithDelay(ctx.network.get(readNodeId), request, self);
                }
            }
        }

        if (ctx.pendingJoinReads == 0) {
            finishJoinPhase();
        }
    }

    private void finishJoinPhase() {
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

    public void onJoinedNetworkMsg(JoinedNetworkMsg msg, ActorRef sender) {
        ctx.network.put(msg.joinedNodeId(), sender);

        var iterator = ctx.storage.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Integer, StorageData> entry = iterator.next();

            if (!ctx.network.isResponsible(ctx.id, entry.getKey(), Constraints.N)) {
                iterator.remove();
            }
        }
    }

    public void onLeaveMsg(LeaveMsg msg) {
        CircularTreeMap<Integer, ActorRef> nextRing = ctx.network.cloneWithout(ctx.id);
        HashMap<Integer, TreeMap<Integer, StorageData>> handoffPackages = new HashMap<>();

        for (Map.Entry<Integer, StorageData> entry : ctx.storage.entrySet()) {
            int dataKey = entry.getKey();

            for (int currentNodeId : nextRing.getNResponsibleNodes(dataKey, Constraints.N)) {
                handoffPackages.putIfAbsent(currentNodeId, new TreeMap<>());
                handoffPackages.get(currentNodeId).put(dataKey, entry.getValue());
            }
        }

        for (Map.Entry<Integer, TreeMap<Integer, StorageData>> entry : handoffPackages.entrySet()) {
            ActorRef targetNode = ctx.network.get(entry.getKey());
            NodeLeavingMsg nodeLeavingMsg = new NodeLeavingMsg(ctx.id, entry.getValue());
            sendWithDelay(targetNode, nodeLeavingMsg, self);
        }

        for (Map.Entry<Integer, ActorRef> entry : nextRing.entrySet()) {
            if (!handoffPackages.containsKey(entry.getKey())) {
                NodeLeavingMsg nodeLeavingMsg = new NodeLeavingMsg(ctx.id, new TreeMap<>());
                sendWithDelay(entry.getValue(), nodeLeavingMsg, self);
            }
        }

        actorContext.stop(self);
    }

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

    public void onCrashMsg(CrashMsg msg) {
        actorContext.stop(self);
    }

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
}
