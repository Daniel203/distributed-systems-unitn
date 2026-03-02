package it.unitn;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.pattern.Patterns;
import it.unitn.constraints.Constraints;
import it.unitn.distributed.Client;
import it.unitn.distributed.Manager;
import it.unitn.models.NodeContext;
import it.unitn.models.Messages.GetStateMsg;
import it.unitn.models.Messages.NodeStateReplyMsg;

public abstract class SystemTestBase {

    protected Manager manager;
    protected ActorRef dummyClient;

    @BeforeEach
    public void setup() {
        NodeContext.formatAllDisks();
        manager = new Manager();
        dummyClient = manager.getSystem().actorOf(Props.create(Client.class), "dummyClient");
    }

    @AfterEach
    public void teardown() {
        manager.getSystem().terminate();
        manager.getSystem().getWhenTerminated().toCompletableFuture().join();
    }

    /**
     * Join a specific number of nodes with IDs following the pattern 0, 10, 20,
     * ..., (count-1)*10.
     * 
     * @param count The number of nodes to boot.
     * @return A list containing the IDs of the booted nodes.
     * @throws InterruptedException If the thread is interrupted while sleeping.
     */
    protected List<Integer> joinNodes(int count) throws InterruptedException {
        List<Integer> nodeIds = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            int nodeId = i * 10;
            manager.join(nodeId);
            nodeIds.add(nodeId);
            Thread.sleep(200);
        }

        Thread.sleep(500);
        return nodeIds;
    }

    /**
     * Sends a GetStateMsg to the node with the specified ID and waits for a
     * NodeStateReplyMsg response.
     * 
     * @param nodeId The ID of the node to query.
     * @return The NodeStateReplyMsg received from the node.
     * @throws Exception If an error occurs while sending the message or waiting for
     *                   the response.
     */
    protected NodeStateReplyMsg getNodeState(int nodeId) throws Exception {
        return (NodeStateReplyMsg) Patterns.ask(
                manager.getNodeById(nodeId),
                new GetStateMsg(),
                Duration.ofSeconds(3)).toCompletableFuture().get();
    }

    /**
     * Verifies that the data for a specific key is correctly replicated across the
     * nodes with the given IDs. It checks that exactly N replicas exist, and that
     * each replica has the expected value and version.
     * 
     * @param nodeIds              The list of node IDs to check for replication.
     * @param key                  The key whose replication is to be verified.
     * @param expectedValue        The expected value for the key in each replica.
     * @param expectedVersion      The expected version for the key in each replica.
     * @param expectedReplicaCount The expected number of replicas
     */
    protected void verifyReplication(List<Integer> nodeIds, int key, String expectedValue, int expectedVersion,
            int expectedReplicaCount) {
        int replicaCount = 0;

        for (int nodeId : nodeIds) {
            try {
                NodeStateReplyMsg state = getNodeState(nodeId);

                if (state.storage().containsKey(key)) {
                    replicaCount++;
                    assertEquals(expectedValue, state.storage().get(key).value(),
                            "Node " + nodeId + " has wrong value for key " + key);
                    assertEquals(expectedVersion, state.storage().get(key).version(),
                            "Node " + nodeId + " has wrong version for key " + key);
                }
            } catch (Exception e) {
                // Ignore
            }
        }

        assertEquals(expectedReplicaCount, replicaCount,
                "Data for key " + key + " should be replicated exactly N=" + Constraints.N + " times!");
    }

    /**
     * Overloaded helper method that assumes the expected replica count is equal to
     * Constraints.N.
     */
    protected void verifyReplication(List<Integer> nodeIds, int key, String expectedValue, int expectedVersion) {
        verifyReplication(nodeIds, key, expectedValue, expectedVersion, Constraints.N);
    }

    /**
     * Helper to find a node that is currently storing the specified key.
     *
     * @param activeNodes The list of active node IDs to check.
     * @param key         The key to look for.
     * @return The ID of a node that is currently responsible for the key.
     * @throws Exception If no active node is found that holds the key.
     */
    protected int getAResponsibleNode(List<Integer> activeNodes, int key) throws Exception {
        for (int nodeId : activeNodes) {
            NodeStateReplyMsg state = getNodeState(nodeId);
            if (state.storage().containsKey(key)) {
                return nodeId;
            }
        }
        throw new RuntimeException("Test Error: No active nodes hold key " + key);
    }
}
