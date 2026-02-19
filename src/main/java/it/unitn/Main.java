package it.unitn;


import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.distributed.Client;
import it.unitn.distributed.Manager;
import it.unitn.model.message.Messages.*;

public class Main {
    public static void main(String[] args) {
        Manager manager = new Manager();

        // Create nodes
        manager.join(10);
        manager.join(20);
        manager.join(30);
        manager.join(40);
        manager.join(50);

        // Create a client
        ActorRef c1 = manager.getSystem()
                .actorOf(Props.create(Client.class), "client1");

        // Send updates
        c1.tell(new ClientUpdateRequestMsg(20, "test1"), manager.getNodeById(10));
        c1.tell(new ClientUpdateRequestMsg(30, "test2"), manager.getNodeById(10));
        c1.tell(new ClientUpdateRequestMsg(40, "test3"), manager.getNodeById(10));
    }

}
