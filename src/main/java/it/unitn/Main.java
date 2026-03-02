package it.unitn;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.distributed.Client;
import it.unitn.distributed.Manager;
import it.unitn.models.Messages.*;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        Manager manager = new Manager();

        int[] initialNodes = {10, 20, 30, 40, 50};
        for (int id : initialNodes) {
            manager.join(id);
            Thread.sleep(800); 
        }

        ActorRef c1 = manager.getSystem().actorOf(Props.create(Client.class), "client1");

        manager.getNodeById(10).tell(new ClientUpdateRequestMsg(15, "pre_crash"), c1);
        Thread.sleep(1500); 

        manager.crash(20);
        Thread.sleep(1000);

        manager.getNodeById(10).tell(new ClientUpdateRequestMsg(15, "post_crash"), c1);
        Thread.sleep(1000);

        System.out.println("State before node 20 recovering");
        int[] finalNodes2 = {10, 20, 30, 40, 50}; 
        for (int id : finalNodes2) {
            ActorRef node = manager.getNodeById(id);
            if (node != null) {
                node.tell(new DebugPrintStateMsg(), ActorRef.noSender());
            }
            Thread.sleep(100); 
        }


        manager.recover(20);
        Thread.sleep(2000);

        System.out.println("Final state of nodes:");
        int[] finalNodes = {10, 20, 30, 40, 50}; 
        for (int id : finalNodes) {
            ActorRef node = manager.getNodeById(id);
            if (node != null) {
                node.tell(new DebugPrintStateMsg(), ActorRef.noSender());
            }
            Thread.sleep(100); 
        }

        Thread.sleep(1000);
        manager.getSystem().terminate();
        manager.getSystem().getWhenTerminated().toCompletableFuture().join();
    }
}
