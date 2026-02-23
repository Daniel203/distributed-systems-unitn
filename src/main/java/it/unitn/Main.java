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
        System.out.println(" PHASE 1: BOOTSTRAPPING INITIAL NETWORK");
        System.out.println("=========================================");
        int[] initialNodes = {10, 20, 30, 40};
        for (int id : initialNodes) {
            System.out.println("Joining node " + id + "...");
            manager.join(id);
            Thread.sleep(1000); 
        }

        ActorRef c1 = manager.getSystem().actorOf(Props.create(Client.class), "client1");
        ActorRef c2 = manager.getSystem().actorOf(Props.create(Client.class), "client2");

        System.out.println("\n=========================================");
        System.out.println(" PHASE 2: CONCURRENT WRITES");
        System.out.println("=========================================");
        System.out.println("[Client 1] asking Node 10 to store key 25 with value 'rock123'...");
        System.out.println("[Client 2] asking Node 20 to store key 35 with value 'water'...");
        
        // Fired concurrently
        manager.getNodeById(10).tell(new ClientUpdateRequestMsg(25, "rock123"), c1);
        manager.getNodeById(20).tell(new ClientUpdateRequestMsg(35, "water"), c2);
        Thread.sleep(1500); 

        System.out.println("\n=========================================");
        System.out.println(" PHASE 3: CONCURRENT READS");
        System.out.println("=========================================");
        System.out.println("[Client 1] asking Node 40 for key 25 (Expected: rock123)");
        System.out.println("[Client 2] asking Node 40 for key 35 (Expected: water)");
        System.out.println("[Client 1] asking Node 30 for key 99 (Expected: KEY_NOT_FOUND)");

        // Fired concurrently to the same Coordinator to test UUID map separation
        manager.getNodeById(40).tell(new ClientGetRequestMsg(25), c1);
        manager.getNodeById(40).tell(new ClientGetRequestMsg(35), c2);
        manager.getNodeById(30).tell(new ClientGetRequestMsg(99), c1);
        Thread.sleep(1500);

        System.out.println("\n=========================================");
        System.out.println(" PHASE 4: DATA UPDATE (TESTING VERSIONS)");
        System.out.println("=========================================");
        System.out.println("[Client 1] asking Node 30 to update key 25 to 'diamond456'...");
        manager.getNodeById(30).tell(new ClientUpdateRequestMsg(25, "diamond456"), c1);
        Thread.sleep(1500);
        
        System.out.println("[Client 2] asking Node 10 to read key 25 (Expected: diamond456)...");
        manager.getNodeById(10).tell(new ClientGetRequestMsg(25), c2);
        Thread.sleep(1500);

        System.out.println("\n=========================================");
        System.out.println(" PHASE 5: LATE NODE JOIN (DATA TRANSFER)");
        System.out.println("=========================================");
        System.out.println("Joining node 50... It should fetch existing data from its neighbor!");
        manager.join(50);
        Thread.sleep(2000); // Give it a bit longer to do the background sync

        System.out.println("\n=========================================");
        System.out.println(" FINAL NODE STATES");
        System.out.println("=========================================");
        int[] allNodes = {10, 20, 30, 40, 50};
        for (int id : allNodes) {
            manager.getNodeById(id).tell(new DebugPrintStateMsg(), ActorRef.noSender());
            Thread.sleep(100); 
        }

        Thread.sleep(1000);
        manager.getSystem().terminate();
        manager.getSystem().getWhenTerminated().toCompletableFuture().join();
    }
}
