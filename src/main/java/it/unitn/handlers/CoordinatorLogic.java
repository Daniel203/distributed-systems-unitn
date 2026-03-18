package it.unitn.handlers;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BinaryOperator;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import it.unitn.constraints.Constraints;
import it.unitn.constraints.Errors;
import it.unitn.models.Messages.CheckTimeoutMsg;
import it.unitn.models.Messages.ClientGetRequestMsg;
import it.unitn.models.Messages.ClientGetResponseMsg;
import it.unitn.models.Messages.ClientUpdateRequestMsg;
import it.unitn.models.Messages.ClientUpdateResponseMsg;
import it.unitn.models.Messages.FinishJoinPhaseMsg;
import it.unitn.models.Messages.ReplicaReadRequestMsg;
import it.unitn.models.Messages.ReplicaReadResponseMsg;
import it.unitn.models.Messages.ReplicaWriteRequestMsg;
import it.unitn.models.Messages.ReplicaWriteResponseMsg;
import it.unitn.models.Messages.UnlockKeyMsg;
import it.unitn.models.NodeContext;
import it.unitn.models.ReadRequestContext;
import it.unitn.models.StorageData;
import it.unitn.models.WriteRequestContext;
import scala.concurrent.duration.Duration;

/**
 * Drives read and write quorum protocols on behalf of clients.
 * Any node can act as coordinator for any key.
 */
public class CoordinatorLogic extends BaseLogic {

    public CoordinatorLogic(NodeContext ctx, ActorContext actorContext, ActorRef self) {
        super(ctx, actorContext, self);
    }

    // ---- Client requests ----------------------------------------------------

    /**
     * Sends ReplicaReadRequestMsg to all N responsible nodes and waits for R
     * replies.
     */
    public void onClientGetRequestMsg(ClientGetRequestMsg msg, ActorRef sender) {
        UUID requestId = UUID.randomUUID();
        ReadRequestContext context = new ReadRequestContext(sender, msg.key(), new ArrayList<>());
        ctx.pendingReads.put(requestId, context);

        startTimeout(requestId);

        for (int targetNodeId : ctx.network.getNResponsibleNodes(msg.key(), Constraints.N)) {
            var request = new ReplicaReadRequestMsg(requestId, msg.key(), false);
            sendWithDelay(ctx.network.get(targetNodeId), request, self);
        }
    }

    /**
     * Queues the request if a write for the same key is already in flight on
     * this coordinator, preventing a symmetric lock deadlock where both writes
     * deny each other's locks. Otherwise starts immediately.
     */
    public void onClientUpdateRequestMsg(ClientUpdateRequestMsg msg, ActorRef sender) {
        if (isWriteInFlightForKey(msg.key())) {
            ctx.waitingWrites
                    .computeIfAbsent(msg.key(), k -> new ArrayDeque<>())
                    .add(Map.entry(msg, sender));
            return;
        }

        startWrite(msg, sender);
    }

    /** Phase 1: lock all N replicas and read their current versions. */
    private void startWrite(ClientUpdateRequestMsg msg, ActorRef sender) {
        UUID requestId = UUID.randomUUID();
        WriteRequestContext context = new WriteRequestContext(sender, msg.key(), msg.value());
        ctx.pendingWrites.put(requestId, context);

        startTimeout(requestId);

        for (int targetNodeId : ctx.network.getNResponsibleNodes(msg.key(), Constraints.N)) {
            var request = new ReplicaReadRequestMsg(requestId, msg.key(), true);
            sendWithDelay(ctx.network.get(targetNodeId), request, self);
        }
    }

    private boolean isWriteInFlightForKey(int key) {
        return ctx.pendingWrites.values().stream()
                .anyMatch(w -> w.key == key);
    }

    /** Starts the next queued write for this key, if any. */
    private void onWriteFinished(int key) {
        ArrayDeque<Map.Entry<ClientUpdateRequestMsg, ActorRef>> queue = ctx.waitingWrites.get(key);
        if (queue != null && !queue.isEmpty()) {
            Map.Entry<ClientUpdateRequestMsg, ActorRef> next = queue.poll();
            startWrite(next.getKey(), next.getValue());
        }
    }

    // ---- Replica responses --------------------------------------------------

    /**
     * Routes to the read or write handler depending on which map owns the
     * requestId.
     */
    public void onReplicaReadResponseMsg(ReplicaReadResponseMsg msg) {
        if (ctx.pendingReads.containsKey(msg.requestId())) {
            handleClientReadQuorum(msg);
        } else if (ctx.pendingWrites.containsKey(msg.requestId())) {
            handleClientWritePhaseOne(msg);
        }
        // Otherwise the operation already timed out.
    }

    /**
     * Collects R replies and returns the highest-versioned value.
     * A lockDenied reply aborts immediately: reading during an active write
     * could expose a partially-written value, violating sequential consistency.
     * When client == self, merges result into storage (background join read).
     */
    private void handleClientReadQuorum(ReplicaReadResponseMsg msg) {
        if (msg.lockDenied()) {
            ReadRequestContext context = ctx.pendingReads.remove(msg.requestId());
            if (context != null && !context.client.equals(self)) {
                sendWithDelay(context.client, new ClientGetResponseMsg(Errors.LOCK_DENIED_MSG), self);
            }
            return;
        }

        ReadRequestContext context = ctx.pendingReads.get(msg.requestId());
        if (context == null) // timed out
            return;

        context.replies.add(msg.data());

        if (context.replies.size() == Constraints.R) {
            StorageData latestData = context.replies.stream()
                    .filter(data -> data != null)
                    .max(Comparator.comparingInt(StorageData::version))
                    .orElse(null);

            if (context.client.equals(self)) {
                // Background join/recovery read: merge into local storage.
                if (latestData != null) {
                    ctx.storage.merge(context.key, latestData,
                            BinaryOperator.maxBy(Comparator.comparingInt(StorageData::version)));
                }

                ctx.pendingJoinReads--;
                if (ctx.pendingJoinReads == 0) {
                    self.tell(new FinishJoinPhaseMsg(), self);
                }
            } else { // Normal client read
                String valueToReturn = (latestData != null) ? latestData.value() : Errors.KEY_NOT_FOUND_MSG;
                sendWithDelay(context.client, new ClientGetResponseMsg(valueToReturn), self);
            }
            ctx.pendingReads.remove(msg.requestId());
        }
    }

    /**
     * Collects W lock-grants (phase 1), then commits to all N replicas (phase 2).
     * A lockDenied before phase 2 aborts the write and releases partial locks.
     * A lockDenied after phase 2 started is ignored (we already have W grants).
     */
    private void handleClientWritePhaseOne(ReplicaReadResponseMsg msg) {
        WriteRequestContext context = ctx.pendingWrites.get(msg.requestId());
        if (context == null) // timed out
            return;

        if (msg.lockDenied()) {
            if (context.phase2Started) // late denial, phase 2 already running
                return;

            ctx.pendingWrites.remove(msg.requestId());
            sendWithDelay(context.client, new ClientUpdateResponseMsg(false), self);

            for (int targetNodeId : ctx.network.getNResponsibleNodes(context.key, Constraints.N)) {
                var unlockMsg = new UnlockKeyMsg(context.key, msg.requestId());
                sendWithDelay(ctx.network.get(targetNodeId), unlockMsg, self);
            }

            onWriteFinished(context.key);
            return;
        }

        context.readReplies.add(msg.data());

        if (context.readReplies.size() == Constraints.W) {
            context.phase2Started = true;

            int maxVersion = context.readReplies.stream()
                    .filter(d -> d != null)
                    .mapToInt(StorageData::version)
                    .max().orElse(0);

            StorageData newData = new StorageData(context.newValue, maxVersion + 1);

            // Reply to client
            sendWithDelay(context.client, new ClientUpdateResponseMsg(true), self);
            // ctx.pendingWrites.remove(msg.requestId());
            // onWriteFinished(context.key);

            // Write to all N (not just W) to maximise availability of the new version.
            for (int targetNodeId : ctx.network.getNResponsibleNodes(context.key, Constraints.N)) {
                var writeRequest = new ReplicaWriteRequestMsg(msg.requestId(), context.key, newData);
                sendWithDelay(ctx.network.get(targetNodeId), writeRequest, self);
            }
            context.readReplies.clear();
        }
    }

    /**
     * Counts W acks; on quorum notifies the client and drains the waiting queue.
     */
    public void onReplicaWriteResponseMsg(ReplicaWriteResponseMsg msg) {
        WriteRequestContext context = ctx.pendingWrites.get(msg.requestId());
        if (context == null) // timed out
            return;

        if (msg.success())
            context.writeAcks++;

        if (context.writeAcks == Constraints.W) {
            // sendWithDelay(context.client, new ClientUpdateResponseMsg(true), self);
            ctx.pendingWrites.remove(msg.requestId());
            onWriteFinished(context.key);
        }
    }

    // ---- Timeout ------------------------------------------------------------

    /**
     * Aborts the operation if it is still pending; releases write locks
     * immediately.
     */
    public void onCheckTimeoutMsg(CheckTimeoutMsg msg) {
        UUID id = msg.requestId();

        if (ctx.pendingReads.containsKey(id)) {
            ReadRequestContext readCtx = ctx.pendingReads.remove(id);
            if (!readCtx.client.equals(self)) {
                sendWithDelay(readCtx.client, new ClientGetResponseMsg(Errors.TIMEOUT_MSG), self);
            }
        }

        if (ctx.pendingWrites.containsKey(id)) {
            WriteRequestContext writeCtx = ctx.pendingWrites.get(id);

            if (!writeCtx.phase2Started) {
                ctx.pendingWrites.remove(id);
                sendWithDelay(writeCtx.client, new ClientUpdateResponseMsg(false), self);
                for (int targetNodeId : ctx.network.getNResponsibleNodes(writeCtx.key, Constraints.N)){
                    var unlockMsg = new UnlockKeyMsg(writeCtx.key, id);
                    sendWithDelay(ctx.network.get(targetNodeId), unlockMsg, self);
                }
                onWriteFinished(writeCtx.key);
            }
        }
    }

    private void startTimeout(UUID requestId) {
        actorContext.system().scheduler().scheduleOnce(
                Duration.create(Constraints.TIMEOUT, TimeUnit.MILLISECONDS),
                self,
                new CheckTimeoutMsg(requestId),
                actorContext.system().dispatcher(),
                ActorRef.noSender());
    }
}
