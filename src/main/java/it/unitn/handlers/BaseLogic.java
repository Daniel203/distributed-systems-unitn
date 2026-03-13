package it.unitn.handlers;

import java.util.concurrent.TimeUnit;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import it.unitn.models.NodeContext;
import scala.concurrent.duration.Duration;

/**
 * Shared base for CoordinatorLogic, ReplicaLogic, and NetworkLogic.
 * Provides access to NodeContext, ActorContext, self, and sendWithDelay.
 */
public abstract class BaseLogic {
    protected final NodeContext ctx;
    protected final ActorContext actorContext;
    protected final ActorRef self;

    public BaseLogic(NodeContext ctx, ActorContext actorContext, ActorRef self) {
        this.ctx = ctx;
        this.actorContext = actorContext;
        this.self = self;
    }

    /**
     * Emulates network propagation delays (10-50ms).
     * Available to all logic classes that extend BaseLogic.
     */
    protected void sendWithDelay(ActorRef target, Object msg, ActorRef sender) {
        int delayMs = 10 + ctx.random.nextInt(41);

        actorContext.system().scheduler().scheduleOnce(
                Duration.create(delayMs, TimeUnit.MILLISECONDS),
                target,
                msg,
                actorContext.system().dispatcher(),
                sender);
    }
}
