package it.unitn.handlers;

import it.unitn.models.NodeContext;
import akka.actor.ActorContext;
import akka.actor.ActorRef;
import it.unitn.constraints.Constraints;
import it.unitn.models.Messages.*;
import it.unitn.models.StorageData;

import java.util.Comparator;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BinaryOperator;
import scala.concurrent.duration.Duration;

public class ReplicaLogic extends BaseLogic {

    public ReplicaLogic(NodeContext ctx, ActorContext actorContext, ActorRef self) {
        super(ctx, actorContext, self);
    }

    public void onReplicaReadRequestMsg(ReplicaReadRequestMsg msg, ActorRef sender) {
        UUID currentLockOwner = ctx.lockedKeys.get(msg.key());
        if (currentLockOwner != null && !currentLockOwner.equals(msg.requestId())) {
            ReplicaReadResponseMsg res = new ReplicaReadResponseMsg(msg.requestId(), msg.key(), null, true);
            sendWithDelay(sender, res, self);
            return;
        }

        if (msg.intentToWrite()) {
            ctx.lockedKeys.put(msg.key(), msg.requestId());
            actorContext.system().scheduler().scheduleOnce(
                    Duration.create(Constraints.TIMEOUT + 500, TimeUnit.MILLISECONDS),
                    self,
                    new UnlockKeyMsg(msg.key(), msg.requestId()),
                    actorContext.system().dispatcher(),
                    ActorRef.noSender());
        }

        StorageData value = ctx.storage.get(msg.key());
        ReplicaReadResponseMsg res = new ReplicaReadResponseMsg(msg.requestId(), msg.key(), value, false);
        sendWithDelay(sender, res, self);
    }

    public void onReplicaWriteRequestMsg(ReplicaWriteRequestMsg msg, ActorRef sender) {
        UUID lockOwner = ctx.lockedKeys.get(msg.key());
        if (lockOwner == null || lockOwner.equals(msg.requestId())) {
            ctx.lockedKeys.remove(msg.key());
            ctx.storage.merge(msg.key(), msg.data(),
                    BinaryOperator.maxBy(Comparator.comparingInt(StorageData::version)));
        }

        ReplicaWriteResponseMsg res = new ReplicaWriteResponseMsg(msg.requestId(), msg.key(), true);
        sendWithDelay(sender, res, self);
    }

    public void onUnlockKeyMsg(UnlockKeyMsg msg) {
        UUID lockOwner = ctx.lockedKeys.get(msg.key());
        if (lockOwner != null && lockOwner.equals(msg.requestId())) {
            ctx.lockedKeys.remove(msg.key());
        }
    }
}
