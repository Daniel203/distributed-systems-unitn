package it.unitn;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.distributed.Client;
import it.unitn.distributed.Manager;
import it.unitn.models.Messages.*;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        Manager manager = new Manager();

        System.out.println("=========================================");
        System.out.println(" PHASE 1: BOOTSTRAPPING NETWORK (10-50)");
        System.out.println("=========================================");
        int[] initialNodes = {10, 20, 30, 40, 50};
        for (int id : initialNodes) {
            manager.join(id);
            Thread.sleep(800); 
        }

        ActorRef c1 = manager.getSystem().actorOf(Props.create(Client.class), "client1");
        ActorRef c2 = manager.getSystem().actorOf(Props.create(Client.class), "client2");
        ActorRef c3 = manager.getSystem().actorOf(Props.create(Client.class), "client3");

        System.out.println("\n=========================================");
        System.out.println(" PHASE 2: HEAVY CONCURRENT WRITES");
        System.out.println("=========================================");
        System.out.println("Clients writing to keys 15, 35, and 55 simultaneously...");
        manager.getNodeById(10).tell(new ClientUpdateRequestMsg(15, "apple"), c1);
        manager.getNodeById(30).tell(new ClientUpdateRequestMsg(35, "banana"), c2);
        manager.getNodeById(50).tell(new ClientUpdateRequestMsg(55, "cherry"), c3);
        Thread.sleep(1500); 

        System.out.println("\n=========================================");
        System.out.println(" PHASE 3: NETWORK CHURN (LEAVE & JOIN)");
        System.out.println("=========================================");
        System.out.println("Node 20 LEAVING gracefully...");
        manager.leave(20);
        Thread.sleep(1500);
        
        System.out.println("Node 60 JOINING the network...");
        manager.join(60);
        Thread.sleep(2000); // Give it time to sync orphaned data

        System.out.println("\n=========================================");
        System.out.println(" PHASE 4: VERIFYING DATA AFTER CHURN");
        System.out.println("=========================================");
        System.out.println("Reading keys 15, 35, and 55...");
        manager.getNodeById(40).tell(new ClientGetRequestMsg(15), c1);
        manager.getNodeById(40).tell(new ClientGetRequestMsg(35), c2);
        manager.getNodeById(40).tell(new ClientGetRequestMsg(55), c3);
        Thread.sleep(1500);

        System.out.println("\n=========================================");
        System.out.println(" PHASE 5: HARD CRASH & FAULT TOLERANCE");
        System.out.println("=========================================");
        System.out.println("Node 40 CRASHES! (Power plug pulled)");
        manager.crash(40);
        Thread.sleep(1000);

        System.out.println("Client 1 updating key 35 to 'banana_v2'...");
        System.out.println("(Node 40 was a replica for 35, but the W=2 quorum should succeed!)");
        manager.getNodeById(10).tell(new ClientUpdateRequestMsg(35, "banana_v2"), c1);
        Thread.sleep(1500);

        System.out.println("\n=========================================");
        System.out.println(" PHASE 6: NODE RECOVERY");
        System.out.println("=========================================");
        System.out.println("Recovering Node 40. It should sync and grab 'banana_v2'...");
        manager.recover(40);
        Thread.sleep(2000); // Wait for recovery sync

        System.out.println("\n=========================================");
        System.out.println(" PHASE 7: FINAL STATE VERIFICATION");
        System.out.println("=========================================");
        int[] finalNodes = {10, 30, 40, 50, 60}; // 20 is gone, 60 is here
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
