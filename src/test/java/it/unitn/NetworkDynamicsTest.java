package it.unitn;

import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import akka.actor.ActorRef;
import it.unitn.constraints.Constraints;
import it.unitn.models.Messages.ClientUpdateRequestMsg;
import it.unitn.models.Messages.ReplicaWriteRequestMsg;
import it.unitn.models.StorageData;

public class NetworkDynamicsTest extends SystemTestBase {

    @Test
    public void testNodeLeaveGracefulHandoff() throws Exception {
        // Arrange: Boot N + 1 nodes
        List<Integer> nodeIds = joinNodes(Constraints.N + 1);

        // Write data to the network
        int testKey = 25;
        manager.getNodeById(nodeIds.get(0)).tell(
                new ClientUpdateRequestMsg(testKey, "handoff_data"),
                dummyClient);
        Thread.sleep(500 + (Constraints.N * 50));

        // Act: Find a node that holds the data and gracefully leave it
        int leavingNodeId = getAResponsibleNode(nodeIds, testKey);
        nodeIds.remove((Integer) leavingNodeId);
        manager.leave(leavingNodeId);
        Thread.sleep(500);

        // Assert: Exactly N nodes should still hold the data
        verifyReplication(nodeIds, testKey, "handoff_data", 1);
    }

    @Test
    public void testNodeCrashFaultTolerance() throws Exception {
        // Arrange: Boot N + 1 nodes
        List<Integer> nodeIds = joinNodes(Constraints.N + 1);

        // Write data to the network
        int testKey = 30;
        manager.getNodeById(nodeIds.get(0)).tell(
                new ClientUpdateRequestMsg(testKey, "v1_data"),
                dummyClient);
        Thread.sleep(500 + (Constraints.N * 50));

        // Act Part 1: Crash a node that holds the data
        int crashedNodeId = getAResponsibleNode(nodeIds, testKey);
        manager.crash(crashedNodeId);
        nodeIds.remove((Integer) crashedNodeId);
        Thread.sleep(500);

        // Act Part 2: Write new data while the node is down
        manager.getNodeById(nodeIds.get(0)).tell(
                new ClientUpdateRequestMsg(testKey, "v2_data"),
                dummyClient);
        Thread.sleep(1500 + (Constraints.N * 50));

        // Assert: The remaining N active nodes must all have the new Version 2
        verifyReplication(nodeIds, testKey, "v2_data", 2, Constraints.N - 1);
    }

    @Test
    public void testNodeRecoverySyncMissedData() throws Exception {
        // Arrange: Boot N + 1 nodes
        List<Integer> nodeIds = joinNodes(Constraints.N + 1);

        // Write data to the network
        int testKey = 15;
        manager.getNodeById(nodeIds.get(0)).tell(
                new ClientUpdateRequestMsg(testKey, "pre_crash"),
                dummyClient);
        Thread.sleep(500 + (Constraints.N * 50));

        // Act Part 1: Crash a responsible node
        int crashedNodeId = getAResponsibleNode(nodeIds, testKey);
        manager.crash(crashedNodeId);
        nodeIds.remove((Integer) crashedNodeId);
        Thread.sleep(500);

        // Act Part 2: Write new data while the node is down
        manager.getNodeById(nodeIds.get(0)).tell(
                new ClientUpdateRequestMsg(testKey, "post_crash"),
                dummyClient);
        Thread.sleep(3000 + (Constraints.N * 50));

        // Act Part 3: Recover the crashed node
        manager.recover(crashedNodeId);
        nodeIds.add(crashedNodeId);
        Thread.sleep(1500); // Wait for recovery and potential synchronization

        // Assert: The recovered node should have the latest data version
        verifyReplication(nodeIds, testKey, "post_crash", 2);
    }

    @Test
    public void testDataHandoffOnNodeJoin() throws Exception {
        // Arrange: Boot N nodes
        List<Integer> nodeIds = joinNodes(Constraints.N);
        int testKey = 25;

        // Write data to the network
        manager.getNodeById(nodeIds.get(0)).tell(
                new ClientUpdateRequestMsg(testKey, "data_to_move"),
                dummyClient);
        Thread.sleep(500 + (Constraints.N * 50));

        // Act: Join a new node to the network.
        // In a ring, adding a node shifts the responsibilities.
        int newNodeId = 5;
        manager.join(newNodeId);
        nodeIds.add(newNodeId);

        // Wait for Join Sync and Data Handoffs to finish
        Thread.sleep(3000 + (Constraints.N * 100));

        // Assert: Exactly N nodes out of the N+1 active nodes hold the data.
        // This implicitly proves that the new node pulled the data, and the node
        // that was pushed out of the Top-N deleted it!
        verifyReplication(nodeIds, testKey, "data_to_move", 1);
    }

    @Test
    public void testJoinPhaseReadsFromQuorumToPreventDataLoss() throws Exception {
        // Arrange: Boot N + 1 nodes
        List<Integer> nodeIds = joinNodes(Constraints.N + 1);

        // Act 1: Write a fresh Version 1 to the network
        manager.getNodeById(nodeIds.get(0)).tell(
                new ClientUpdateRequestMsg(5, "LATEST_DATA"), dummyClient);
        Thread.sleep(500 + (Constraints.N * 50));

        // Act 2: Force Node 10 to have an older, stale version of the data.
        StorageData staleData = new StorageData("STALE_DATA", 0);
        manager.getNodeById(10).tell(new ReplicaWriteRequestMsg(
                UUID.randomUUID(), 5, staleData), ActorRef.noSender());
        Thread.sleep(500);

        // Act 3: Node 5 joins. It will ask its neighbor (Node 10) for data
        manager.join(5);
        nodeIds.add(5);
        Thread.sleep(3000 + (Constraints.N * 100)); 

        // Assert: Node 5 must have discovered "LATEST_DATA" (v1) by doing a background Quorum Read,
        // and it should not have Node 10's "STALE_DATA" (v0).
        verifyReplication(nodeIds, 5, "LATEST_DATA", 1);
    }

}
