package it.unitn;

import akka.actor.ActorRef;
import akka.pattern.Patterns;
import akka.actor.Props;
import it.unitn.distributed.Manager;
import it.unitn.models.Messages.*;
import it.unitn.distributed.Client;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

public class DistributedSystemTest {

    private Manager manager;

    @BeforeEach
    public void setup() {
        manager = new Manager();
    }

    @AfterEach
    public void teardown() {
        manager.getSystem().terminate();
        manager.getSystem().getWhenTerminated().toCompletableFuture().join();
    }

    // =========================================================================
    // 1. NETWORK BOOTSTRAPPING & SCALING
    // =========================================================================

    @Test
    public void testNetworkBootstrapping() throws Exception {
        manager.join(10);
        Thread.sleep(500);
        manager.join(20);
        Thread.sleep(500);
        manager.join(30);
        Thread.sleep(1000);

        ActorRef node10 = manager.getNodeById(10);
        NodeStateReplyMsg reply = (NodeStateReplyMsg) Patterns.ask(node10, new GetStateMsg(), Duration.ofSeconds(3))
                .toCompletableFuture().get();

        assertNotNull(reply);
        assertEquals(3, reply.networkView().size(), "Node 10 should see exactly 3 nodes");
        assertTrue(reply.networkView().containsKey(10));
        assertTrue(reply.networkView().containsKey(20));
        assertTrue(reply.networkView().containsKey(30));
        assertTrue(reply.storage().isEmpty(), "Storage should be empty on startup");
    }

    @Test
    public void testSingleNodeNetwork() throws Exception {
        // Boot ONLY 1 node
        manager.join(10);
        Thread.sleep(1000);

        ActorRef client = manager.getSystem().actorOf(Props.create(Client.class), "testClient");

        // Write data
        manager.getNodeById(10).tell(new ClientUpdateRequestMsg(42, "lonely_node_data"), client);
        Thread.sleep(1500);

        NodeStateReplyMsg state10 = (NodeStateReplyMsg) Patterns.ask(
                manager.getNodeById(10), new GetStateMsg(), Duration.ofSeconds(3)).toCompletableFuture().get();

        // The single node must store the data locally despite N=3
        assertNotNull(state10.storage().get(42), "Node 10 must store the key even if N > network size");
        assertEquals("lonely_node_data", state10.storage().get(42).value());
        assertEquals(1, state10.storage().get(42).version());
    }

    @Test
    public void testJoinPopulatedNetworkWithDataHandoff() throws Exception {
        manager.join(10);
        Thread.sleep(500);
        manager.join(20);
        Thread.sleep(500);
        manager.join(30);
        Thread.sleep(1000);

        ActorRef client = manager.getSystem().actorOf(Props.create(Client.class), "testClient");

        // Key 25 maps to Node 30. Replicas: 30, 10, 20
        manager.getNodeById(10).tell(new ClientUpdateRequestMsg(25, "data_to_move"), client);
        Thread.sleep(1500);

        // Join Node 40. New Replicas for Key 25: 30, 40, 10. Node 20 should DROP it.
        // Node 40 should PULL it.
        manager.join(40);
        Thread.sleep(2000);

        NodeStateReplyMsg state20 = (NodeStateReplyMsg) Patterns
                .ask(manager.getNodeById(20), new GetStateMsg(), Duration.ofSeconds(3)).toCompletableFuture().get();
        NodeStateReplyMsg state40 = (NodeStateReplyMsg) Patterns
                .ask(manager.getNodeById(40), new GetStateMsg(), Duration.ofSeconds(3)).toCompletableFuture().get();

        assertNull(state20.storage().get(25),
                "Node 20 should have dropped Key 25 because it's no longer in the top N=3");
        assertNotNull(state40.storage().get(25), "Node 40 should have pulled Key 25 during join sync");
        assertEquals("data_to_move", state40.storage().get(25).value());
    }

    @Test
    public void testLargeNetworkScale() throws Exception {
        // Boot 10 nodes
        for (int i = 10; i <= 100; i += 10) {
            manager.join(i);
            Thread.sleep(300);
        }
        Thread.sleep(1500);

        ActorRef client = manager.getSystem().actorOf(Props.create(Client.class), "testClient");

        // Key 45 maps to 50. Replicas: 50, 60, 70
        manager.getNodeById(10).tell(new ClientUpdateRequestMsg(45, "large_net_data"), client);
        Thread.sleep(2000);

        NodeStateReplyMsg state50 = (NodeStateReplyMsg) Patterns
                .ask(manager.getNodeById(50), new GetStateMsg(), Duration.ofSeconds(3)).toCompletableFuture().get();
        NodeStateReplyMsg state60 = (NodeStateReplyMsg) Patterns
                .ask(manager.getNodeById(60), new GetStateMsg(), Duration.ofSeconds(3)).toCompletableFuture().get();
        NodeStateReplyMsg state70 = (NodeStateReplyMsg) Patterns
                .ask(manager.getNodeById(70), new GetStateMsg(), Duration.ofSeconds(3)).toCompletableFuture().get();
        NodeStateReplyMsg state80 = (NodeStateReplyMsg) Patterns
                .ask(manager.getNodeById(80), new GetStateMsg(), Duration.ofSeconds(3)).toCompletableFuture().get();

        assertNotNull(state50.storage().get(45), "Node 50 must have Key 45");
        assertNotNull(state60.storage().get(45), "Node 60 must have Key 45");
        assertNotNull(state70.storage().get(45), "Node 70 must have Key 45");
        assertNull(state80.storage().get(45), "Node 80 should NOT have Key 45");
    }

    // =========================================================================
    // 2. UPDATES & VERSIONS
    // =========================================================================

    @Test
    public void testMultipleWritesSameKeyIncrementsVersion() throws Exception {
        manager.join(10);
        Thread.sleep(500);
        manager.join(20);
        Thread.sleep(500);
        manager.join(30);
        Thread.sleep(1000);

        ActorRef client = manager.getSystem().actorOf(Props.create(Client.class), "testClient");

        // Write Version 1
        manager.getNodeById(10).tell(new ClientUpdateRequestMsg(100, "version_1"), client);
        Thread.sleep(1500);

        // Write Version 2 to the SAME key
        manager.getNodeById(20).tell(new ClientUpdateRequestMsg(100, "version_2"), client);
        Thread.sleep(1500);

        NodeStateReplyMsg state30 = (NodeStateReplyMsg) Patterns
                .ask(manager.getNodeById(30), new GetStateMsg(), Duration.ofSeconds(3)).toCompletableFuture().get();

        assertNotNull(state30.storage().get(100));
        assertEquals("version_2", state30.storage().get(100).value(), "Value should be updated to version 2");
        assertEquals(2, state30.storage().get(100).version(), "Version must increment to 2");
    }

    // =========================================================================
    // 3. LEAVES, CRASHES & RECOVERY
    // =========================================================================

    @Test
    public void testNodeLeaveGracefulHandoff() throws Exception {
        manager.join(10);
        Thread.sleep(500);
        manager.join(20);
        Thread.sleep(500);
        manager.join(30);
        Thread.sleep(500);
        manager.join(40);
        Thread.sleep(1000);

        ActorRef client = manager.getSystem().actorOf(Props.create(Client.class), "testClient");

        // Key 25 Replicas: 30, 40, 10
        manager.getNodeById(10).tell(new ClientUpdateRequestMsg(25, "handoff_data"), client);
        Thread.sleep(1500);

        // Leave 30. New Replicas: 40, 10, 20
        manager.leave(30);
        Thread.sleep(2000);

        NodeStateReplyMsg state20 = (NodeStateReplyMsg) Patterns
                .ask(manager.getNodeById(20), new GetStateMsg(), Duration.ofSeconds(3)).toCompletableFuture().get();

        assertNotNull(state20.storage().get(25), "Node 20 must adopt Key 25 after Node 30 leaves");
        assertEquals("handoff_data", state20.storage().get(25).value());
    }

    @Test
    public void testNodeCrashFaultTolerance() throws Exception {
        manager.join(10);
        Thread.sleep(500);
        manager.join(20);
        Thread.sleep(500);
        manager.join(30);
        Thread.sleep(500);
        manager.join(40);
        Thread.sleep(1000);

        ActorRef client = manager.getSystem().actorOf(Props.create(Client.class), "testClient");

        // Key 25 Replicas: 30, 40, 10
        manager.getNodeById(10).tell(new ClientUpdateRequestMsg(25, "v1"), client);
        Thread.sleep(1500);

        // Crash 30! Ring is technically the same to other nodes.
        manager.crash(30);
        Thread.sleep(1000);

        // Write v2. Quorum should succeed using 40 and 10!
        manager.getNodeById(20).tell(new ClientUpdateRequestMsg(25, "v2_after_crash"), client);
        Thread.sleep(1500);

        NodeStateReplyMsg state40 = (NodeStateReplyMsg) Patterns
                .ask(manager.getNodeById(40), new GetStateMsg(), Duration.ofSeconds(3)).toCompletableFuture().get();

        assertNotNull(state40.storage().get(25));
        assertEquals("v2_after_crash", state40.storage().get(25).value(), "Surviving replicas should get the update");
        assertEquals(2, state40.storage().get(25).version());
    }

    @Test
    public void testNodeRecoverySyncsMissedData() throws Exception {
        manager.join(10);
        Thread.sleep(500);
        manager.join(20);
        Thread.sleep(500);
        manager.join(30);
        Thread.sleep(1000);

        ActorRef client = manager.getSystem().actorOf(Props.create(Client.class), "testClient");

        // Key 15 Replicas: 20, 30, 10
        manager.getNodeById(10).tell(new ClientUpdateRequestMsg(15, "pre_crash"), client);
        Thread.sleep(1500);

        manager.crash(20);
        Thread.sleep(1000);

        // Update while 20 is dead
        manager.getNodeById(10).tell(new ClientUpdateRequestMsg(15, "post_crash"), client);
        Thread.sleep(1500);

        // Recover 20
        manager.recover(20);
        Thread.sleep(2000);

        NodeStateReplyMsg state20 = (NodeStateReplyMsg) Patterns
                .ask(manager.getNodeById(20), new GetStateMsg(), Duration.ofSeconds(3)).toCompletableFuture().get();

        assertNotNull(state20.storage().get(15));
        assertEquals("post_crash", state20.storage().get(15).value(),
                "Node 20 must pull the newer version upon recovery");
        assertEquals(2, state20.storage().get(15).version());
    }

    // =========================================================================
    // 4. COMPLEX EDGE CASES
    // =========================================================================

    @Test
    public void testComplexNetworkChurnSequence() throws Exception {
        // Boot 5 nodes
        for (int i = 10; i <= 50; i += 10) {
            manager.join(i);
            Thread.sleep(500);
        }
        Thread.sleep(1000);

        ActorRef client = manager.getSystem().actorOf(Props.create(Client.class), "testClient");

        // 1. Crash Node 40
        manager.crash(40);
        Thread.sleep(1000);

        // 2. Write to Key 35 (Replicas: 40 [dead], 50, 10). Should succeed!
        manager.getNodeById(10).tell(new ClientUpdateRequestMsg(35, "stress_test"), client);
        Thread.sleep(1500);

        // 3. Leave Node 20 gracefully
        manager.leave(20);
        Thread.sleep(1500);

        // 4. Recover Node 40
        manager.recover(40);
        Thread.sleep(2000);

        // Verify final state of Node 40.
        // Replicas for Key 35 in ring [10, 30, 40, 50] are: 40, 50, 10.
        // Node 40 should have pulled "stress_test" during recovery.
        NodeStateReplyMsg state40 = (NodeStateReplyMsg) Patterns
                .ask(manager.getNodeById(40), new GetStateMsg(), Duration.ofSeconds(3)).toCompletableFuture().get();

        assertNotNull(state40.storage().get(35), "Node 40 must have recovered Key 35");
        assertEquals("stress_test", state40.storage().get(35).value());
    }

    @Test
    public void testSequentialConsistencyRejectsOverlappingWrites() throws Exception {
        // Arrange
        manager.join(10);
        manager.join(20);
        manager.join(30);
        Thread.sleep(1500);

        // Act: Fire two updates for the same key to the same coordinator at the exact
        // same time
        // We use Patterns.ask so the coordinator replies directly to the test
        CompletableFuture<Object> write1 = Patterns.ask(
                manager.getNodeById(10),
                new ClientUpdateRequestMsg(42, "first_write"),
                Duration.ofSeconds(3)).toCompletableFuture();

        CompletableFuture<Object> write2 = Patterns.ask(
                manager.getNodeById(10),
                new ClientUpdateRequestMsg(42, "second_write"),
                Duration.ofSeconds(3)).toCompletableFuture();

        // Wait for both to finish
        ClientUpdateResponseMsg res1 = (ClientUpdateResponseMsg) write1.get();
        ClientUpdateResponseMsg res2 = (ClientUpdateResponseMsg) write2.get();

        // Assert: To prevent split-brain and sequential consistency violations,
        // the node must lock the key. One request should succeed, the other MUST fail.
        boolean oneSucceededOneFailed = (res1.success() && !res2.success()) || (!res1.success() && res2.success());

        assertTrue(oneSucceededOneFailed,
                "Sequential Consistency Violation: Both concurrent writes were accepted! One should have been rejected.");
    }

    @Test
    public void testJoinPhaseReadsFromQuorumToPreventDataLoss() throws Exception {
        // Arrange: Boot initial network
        manager.join(10);
        Thread.sleep(500);
        manager.join(20);
        Thread.sleep(500);
        manager.join(30);
        Thread.sleep(500);
        manager.join(40);
        Thread.sleep(1500);

        // Act 1: Write Key 5 (Replicas in ring [10,20,30,40] are: 10, 20, 30).
        // This makes Version 1.
        ActorRef client = manager.getSystem().actorOf(Props.create(Client.class), "testClient");
        manager.getNodeById(40).tell(new ClientUpdateRequestMsg(5, "LATEST_DATA"), client);
        Thread.sleep(1500);

        // Act 2: Force Node 10 to have an older, stale version of Key 5.
        // We do this by sending a direct internal write, bypassing the coordinator.
        it.unitn.models.StorageData staleData = new it.unitn.models.StorageData("STALE_DATA", 0);
        manager.getNodeById(10).tell(new ReplicaWriteRequestMsg(java.util.UUID.randomUUID(), 5, staleData),
                ActorRef.noSender());
        Thread.sleep(500);

        // At this exact moment:
        // Node 10 has v0 ("STALE_DATA")
        // Node 20 has v1 ("LATEST_DATA")
        // Node 30 has v1 ("LATEST_DATA")

        // Act 3: Node 5 joins. It will ask its clockwise neighbor (Node 10) for data!
        manager.join(5);
        Thread.sleep(2000);

        // Extract Node 5's state to see what it did
        NodeStateReplyMsg state5 = (NodeStateReplyMsg) Patterns.ask(
                manager.getNodeById(5), new GetStateMsg(), Duration.ofSeconds(3)).toCompletableFuture().get();

        // Assert: Node 5 must take responsibility for Key 5
        assertNotNull(state5.storage().get(5), "Node 5 must take responsibility for Key 5");

        // Assert: Node 5 must have discovered "LATEST_DATA" (v1) during its join phase
        // by reading from the quorum,
        // and NOT blindly trusted Node 10's "STALE_DATA" (v0).
        assertEquals("LATEST_DATA", state5.storage().get(5).value(),
                "Join Sync Violation: Node 5 pulled stale data from its neighbor instead of reading from a quorum!");
    }
}
