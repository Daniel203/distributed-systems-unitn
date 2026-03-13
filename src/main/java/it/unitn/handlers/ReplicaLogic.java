package it.unitn.handlers;

import java.util.Comparator;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BinaryOperator;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import it.unitn.constraints.Constraints;
import it.unitn.models.Messages.ReplicaReadRequestMsg;
import it.unitn.models.Messages.ReplicaReadResponseMsg;
import it.unitn.models.Messages.ReplicaWriteRequestMsg;
import it.unitn.models.Messages.ReplicaWriteResponseMsg;
import it.unitn.models.Messages.UnlockKeyMsg;
import it.unitn.models.NodeContext;
import it.unitn.models.StorageData;
import scala.concurrent.duration.Duration;

/**
 * Manages key locking and local storage on behalf of write/read coordinators.
 */
public class ReplicaLogic extends BaseLogic {

    public ReplicaLogic(NodeContext ctx, ActorContext actorContext, ActorRef self) {
        super(ctx, actorContext, self);
    }

    /**
     * Denies if the key is locked by a different request.
     * If intentToWrite, acquires the lock and schedules a safety auto-unlock
     * (T+500ms) in case the coordinator crashes before sending phase 2.
     */
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

    /**
     * Writes only if this replica owns the lock. 
     * Reports the actual outcome so the coordinator counts only true commits.
     */
    public void onReplicaWriteRequestMsg(ReplicaWriteRequestMsg msg, ActorRef sender) {
        UUID lockOwner = ctx.lockedKeys.get(msg.key());
        boolean wrote = false;

        if (lockOwner == null || lockOwner.equals(msg.requestId())) {
            ctx.lockedKeys.remove(msg.key());
            ctx.storage.merge(msg.key(), msg.data(),
                    BinaryOperator.maxBy(Comparator.comparingInt(StorageData::version)));
            wrote = true;
        }

        ReplicaWriteResponseMsg res = new ReplicaWriteResponseMsg(msg.requestId(), msg.key(), wrote);
        sendWithDelay(sender, res, self);
    }

    /**
     * Releases the lock only if the UUID matches the current owner,
     * preventing a stale unlock from releasing a lock held by a newer write.
     */
    public void onUnlockKeyMsg(UnlockKeyMsg msg) {
        UUID lockOwner = ctx.lockedKeys.get(msg.key());
        if (lockOwner != null && lockOwner.equals(msg.requestId())) {
            ctx.lockedKeys.remove(msg.key());
        }
    }
}
