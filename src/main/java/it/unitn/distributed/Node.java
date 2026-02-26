package it.unitn.distributed;

import akka.actor.AbstractActor;
import akka.actor.Props;
import it.unitn.handlers.CoordinatorLogic;
import it.unitn.handlers.NetworkLogic;
import it.unitn.handlers.ReplicaLogic;
import it.unitn.models.NodeContext;
import it.unitn.models.Messages.*;

public class Node extends AbstractActor {
    // The Shared State
    private final NodeContext ctx;

    // The Logic Modules
    private final CoordinatorLogic coordinator;
    private final ReplicaLogic replica;
    private final NetworkLogic network;

    public Node(int id) {
        super();
        this.ctx = new NodeContext(id);

        this.coordinator = new CoordinatorLogic(ctx, getContext(), getSelf());
        this.replica = new ReplicaLogic(ctx, getContext(), getSelf());
        this.network = new NetworkLogic(ctx, getContext(), getSelf());
    }

    public static Props props(int id) {
        return Props.create(Node.class, () -> new Node(id));
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                // --- COORDINATOR ROUTING ---
                .match(ClientGetRequestMsg.class, msg -> coordinator.onClientGetRequestMsg(msg, getSender()))
                .match(ClientUpdateRequestMsg.class, msg -> coordinator.onClientUpdateRequestMsg(msg, getSender()))
                .match(ReplicaReadResponseMsg.class, coordinator::onReplicaReadResponseMsg)
                .match(ReplicaWriteResponseMsg.class, coordinator::onReplicaWriteResponseMsg)
                .match(CheckTimeoutMsg.class, coordinator::onCheckTimeoutMsg)

                // --- REPLICA ROUTING ---
                .match(ReplicaReadRequestMsg.class, msg -> replica.onReplicaReadRequestMsg(msg, getSender()))
                .match(ReplicaWriteRequestMsg.class, msg -> replica.onReplicaWriteRequestMsg(msg, getSender()))
                .match(UnlockKeyMsg.class, replica::onUnlockKeyMsg)

                // --- NETWORK ROUTING ---
                .match(JoinMsg.class, network::onJoinMsg)
                .match(LeaveMsg.class, network::onLeaveMsg)
                .match(NodeListRequestMsg.class, msg -> network.onNodeListRequestMsg(msg, getSender()))
                .match(NodeListResponseMsg.class, network::onNodeListResponseMsg)
                .match(StorageRequestMsg.class, msg -> network.onStorageRequestMsg(msg, getSender()))
                .match(StorageResponseMsg.class, network::onStorageResponseMsg)
                .match(JoinedNetworkMsg.class, msg -> network.onJoinedNetworkMsg(msg, getSender()))
                .match(NodeLeavingMsg.class, network::onNodeLeavingMsg)
                .match(CrashMsg.class, network::onCrashMsg)
                .match(RecoverMsg.class, network::onRecoverMsg)
                .match(FinishJoinPhaseMsg.class, network::onFinishJoinPhaseMsg)

                // --- DEBUG ROUTING ---
                .match(DebugPrintStateMsg.class, this::onDebugPrintStateMsg)
                .match(GetStateMsg.class, this::onGetStateMsg)
                .build();
    }

    private void onDebugPrintStateMsg(DebugPrintStateMsg msg) {
        System.out.println("=========================================");
        System.out.println("NODE " + ctx.id + " STATE:");

        // Print the network view
        System.out.print("  Network View: [");
        for (Integer nodeId : ctx.network.getMap().keySet()) {
            System.out.print(nodeId + " ");
        }
        System.out.println("]");

        // Print the stored data
        System.out.println("  Storage:");
        if (ctx.storage.isEmpty()) {
            System.out.println("    (empty)");
        } else {
            for (java.util.Map.Entry<Integer, it.unitn.models.StorageData> entry : ctx.storage.entrySet()) {
                System.out.println("    Key: " + entry.getKey() +
                        " | Value: '" + entry.getValue().value() +
                        "' | Version: " + entry.getValue().version());
            }
        }
        System.out.println("=========================================\n");
    }

    private void onGetStateMsg(GetStateMsg msg) {
        getSender().tell(new NodeStateReplyMsg(new java.util.TreeMap<>(ctx.storage), ctx.network.getMap()), getSelf());
    }
}
