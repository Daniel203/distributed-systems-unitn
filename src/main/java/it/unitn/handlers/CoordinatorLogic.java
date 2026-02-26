package it.unitn.handlers;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import it.unitn.constraints.Constraints;
import it.unitn.constraints.Errors;
import it.unitn.models.*;
import it.unitn.models.Messages.*;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import scala.concurrent.duration.Duration;

public class CoordinatorLogic extends BaseLogic {

    public CoordinatorLogic(NodeContext ctx, ActorContext actorContext, ActorRef self) {
        super(ctx, actorContext, self);
    }

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

    public void onClientUpdateRequestMsg(ClientUpdateRequestMsg msg, ActorRef sender) {
        UUID requestId = UUID.randomUUID();
        WriteRequestContext context = new WriteRequestContext(sender, msg.key(), msg.value());
        ctx.pendingWrites.put(requestId, context);

        startTimeout(requestId);

        for (int targetNodeId : ctx.network.getNResponsibleNodes(msg.key(), Constraints.N)) {
            var request = new ReplicaReadRequestMsg(requestId, msg.key(), true);
            sendWithDelay(ctx.network.get(targetNodeId), request, self);
        }
    }

    public void onReplicaReadResponseMsg(ReplicaReadResponseMsg msg) {
        if (ctx.pendingReads.containsKey(msg.requestId())) {
            handleClientReadQuorum(msg);
        } else if (ctx.pendingWrites.containsKey(msg.requestId())) {
            handleClientWritePhaseOne(msg);
        }
    }

    private void handleClientReadQuorum(ReplicaReadResponseMsg msg) {
        if (msg.lockDenied()) {
            ReadRequestContext context = ctx.pendingReads.remove(msg.requestId());
            if (context != null && !context.client.equals(self)) {
                sendWithDelay(context.client, new ClientGetResponseMsg(Errors.LOCK_DENIED_MSG), self);
            }
            return;
        }

        ReadRequestContext context = ctx.pendingReads.get(msg.requestId());
        if (context == null) return;

        context.replies.add(msg.data());

        if (context.replies.size() == Constraints.R) {
            StorageData latestData = context.replies.stream()
                    .filter(data -> data != null)
                    .max(Comparator.comparingInt(StorageData::version))
                    .orElse(null);

            if (context.client.equals(self)) { // Background join read
                if (latestData != null) {
                    ctx.storage.put(context.key, new StorageData(latestData.value(), latestData.version()));
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

    private void handleClientWritePhaseOne(ReplicaReadResponseMsg msg) {
        WriteRequestContext context = ctx.pendingWrites.get(msg.requestId());
        if (context == null) return;

        if (msg.lockDenied()) {
            if (context.phase2Started) return;

            ctx.pendingWrites.remove(msg.requestId());
            sendWithDelay(context.client, new ClientUpdateResponseMsg(false), self);

            for (int targetNodeId : ctx.network.getNResponsibleNodes(context.key, Constraints.N)) {
                var unlockMsg = new UnlockKeyMsg(context.key, msg.requestId());
                sendWithDelay(ctx.network.get(targetNodeId), unlockMsg, self);
            }
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

            for (int targetNodeId : ctx.network.getNResponsibleNodes(context.key, Constraints.N)) {
                var writeRequest = new ReplicaWriteRequestMsg(msg.requestId(), context.key, newData);
                sendWithDelay(ctx.network.get(targetNodeId), writeRequest, self);
            }
            context.readReplies.clear();
        }
    }

    public void onReplicaWriteResponseMsg(ReplicaWriteResponseMsg msg) {
        WriteRequestContext context = ctx.pendingWrites.get(msg.requestId());
        if (context == null) return;

        if (msg.success()) context.writeAcks++;

        if (context.writeAcks == Constraints.W) {
            sendWithDelay(context.client, new ClientUpdateResponseMsg(true), self);
            ctx.pendingWrites.remove(msg.requestId());
        }
    }

    public void onCheckTimeoutMsg(CheckTimeoutMsg msg) {
        UUID id = msg.requestId();

        if (ctx.pendingReads.containsKey(id)) {
            ReadRequestContext readCtx = ctx.pendingReads.remove(id);
            if (!readCtx.client.equals(self)) {
                sendWithDelay(readCtx.client, new ClientGetResponseMsg(Errors.TIMEOUT_MSG), self);
            }
        }

        if (ctx.pendingWrites.containsKey(id)) {
            WriteRequestContext writeCtx = ctx.pendingWrites.remove(id);
            sendWithDelay(writeCtx.client, new ClientUpdateResponseMsg(false), self);
        }
    }

    private void startTimeout(UUID requestId) {
        actorContext.system().scheduler().scheduleOnce(
                Duration.create(Constraints.TIMEOUT, TimeUnit.MILLISECONDS),
                self,
                new CheckTimeoutMsg(requestId),
                actorContext.system().dispatcher(),
                ActorRef.noSender()
        );
    }
}
