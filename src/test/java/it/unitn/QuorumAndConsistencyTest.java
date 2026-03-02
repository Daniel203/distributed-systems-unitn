package it.unitn;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;

import akka.pattern.Patterns;
import it.unitn.constraints.Constraints;
import it.unitn.constraints.Errors;
import it.unitn.models.Messages.ClientGetRequestMsg;
import it.unitn.models.Messages.ClientGetResponseMsg;
import it.unitn.models.Messages.ClientUpdateRequestMsg;
import it.unitn.models.Messages.ClientUpdateResponseMsg;
import it.unitn.models.Messages.NodeStateReplyMsg;

public class QuorumAndConsistencyTest extends SystemTestBase {
    @Test
    public void testSuccessfulWriteAndReadQuorum() throws Exception {
        // Arrange: Boot N nodes
        List<Integer> nodeIds = joinNodes(Constraints.N);
        int targetKey = 55;
        String targetValue = "quorum_test_data";

        // Act 1: Send a write request and wait for coordinator's response
        ClientUpdateResponseMsg writeRes = (ClientUpdateResponseMsg) Patterns.ask(
                manager.getNodeById(nodeIds.get(0)),
                new ClientUpdateRequestMsg(targetKey, targetValue),
                Duration.ofMillis(Constraints.TIMEOUT + 1500)).toCompletableFuture().join();

        // Assert 1: The W quorum was reached
        assertTrue(writeRes.success(), "Write operation should succeed when N nodes are active.");

        // Act 2: Send a read request and wait for coordinator's response
        ClientGetResponseMsg readRes = (ClientGetResponseMsg) Patterns.ask(
                manager.getNodeById(nodeIds.get(0)),
                new ClientGetRequestMsg(targetKey),
                Duration.ofMillis(Constraints.TIMEOUT + 1500)).toCompletableFuture().get();

        // Assert 2: The R quorum was reached and the value is correct
        assertEquals(targetValue, readRes.data(), "Read operation should return the recently written data.");

        // Assert 3: Verify the data is correctly replicated
        verifyReplication(nodeIds, targetKey, targetValue, 1);
    }

    @Test
    public void testOperationFailsWhenQuorumNotReached() throws Exception {
        // Arrange: Boot N nodes
        List<Integer> nodeIds = joinNodes(Constraints.N);
        int targetKey = 99;

        // Act 1: Mathematically calculate how many nodes to crash to make quorum W
        // impossible. If N=3 and W=2, we must crash 2 nodes (leaving 1 active node).
        int nodesToCrash = Constraints.N - Constraints.W + 1;
        for (int i = 0; i < nodesToCrash; i++) {
            int crashedNodeId = nodeIds.remove(i);
            manager.crash(crashedNodeId);
        }
        Thread.sleep(500);

        // Act 2: Ask the survived node to perform a write operation
        ClientUpdateResponseMsg writeRes = (ClientUpdateResponseMsg) Patterns.ask(
                manager.getNodeById(nodeIds.get(0)),
                new ClientUpdateRequestMsg(targetKey, "unreachable_data"),
                Duration.ofMillis(Constraints.TIMEOUT + 1500)).toCompletableFuture().join();

        // Assert: The coordinator must hit timeout T and retrun false
        assertFalse(writeRes.success(), "Write should fail because the W quorum cannot be reached!");
    }

    @Test
    public void testSequentialConsistencyConcurrentWrites() throws Exception {
        // Arrange: Boot N nodes
        List<Integer> nodeIds = joinNodes(Constraints.N);
        int targetKey = 42;

        // Act: Fire two updates for the exact same key at the exact same time
        CompletableFuture<Object> write1 = Patterns.ask(
                manager.getNodeById(nodeIds.get(0)),
                new ClientUpdateRequestMsg(targetKey, "first_write"),
                Duration.ofMillis(Constraints.TIMEOUT + 2000)).toCompletableFuture();

        CompletableFuture<Object> write2 = Patterns.ask(
                manager.getNodeById(nodeIds.get(0)),
                new ClientUpdateRequestMsg(targetKey, "second_write"),
                Duration.ofMillis(Constraints.TIMEOUT + 2000)).toCompletableFuture();

        // Wait for both concurrent operations to resolve
        ClientUpdateResponseMsg res1 = (ClientUpdateResponseMsg) write1.get();
        ClientUpdateResponseMsg res2 = (ClientUpdateResponseMsg) write2.get();

        boolean bothSucceeded = res1.success() && res2.success();
        boolean oneSucceeded = res1.success() ^ res2.success();

        // Assert 1: The system must not crash or fail both writes
        assertTrue(bothSucceeded || oneSucceeded,
                "Sequential Consistency Violation: Both operations failed abnormally!");

        // Fetch the final state of the coordinator's disk to prove sequential
        // consistency
        NodeStateReplyMsg finalState = getNodeState(nodeIds.get(0));

        // Assert 2: Verify the 2PL algorithm correctly ordered the operations
        if (bothSucceeded) {
            // If random delays perfectly serialized them (Write 1 finished -> Write 2
            // started),
            // the version must have safely incremented to 2!
            assertEquals(2, finalState.storage().get(targetKey).version(),
                    "Both succeeded sequentially, but the version is corrupted!");
        } else {
            // If they truly overlapped, the 2PL locks will reject one of them.
            // The surviving write must be cleanly saved as Version 1!
            assertEquals(1, finalState.storage().get(targetKey).version(),
                    "One write was rejected, but the surviving write's version is corrupted!");
        }
    }

    @Test
    public void testSequentialConsistencyMultipleReaders() throws Exception {
        // Arrange: Boot N nodes
        List<Integer> nodeIds = joinNodes(Constraints.N);
        int targetKey = 77;

        // Act 1: Process 1 & Process 2: Concurrent Writers
        CompletableFuture<Object> writeA = Patterns.ask(
                manager.getNodeById(nodeIds.get(0)), // Writer 1
                new ClientUpdateRequestMsg(targetKey, "A"),
                Duration.ofMillis(Constraints.TIMEOUT + 2000)).toCompletableFuture();

        CompletableFuture<Object> writeB = Patterns.ask(
                manager.getNodeById(nodeIds.get(1)), // Writer 2
                new ClientUpdateRequestMsg(targetKey, "B"),
                Duration.ofMillis(Constraints.TIMEOUT + 2000)).toCompletableFuture();

        ClientUpdateResponseMsg resA = (ClientUpdateResponseMsg) writeA.get();
        ClientUpdateResponseMsg resB = (ClientUpdateResponseMsg) writeB.get();

        // Act 2: Process 3 & Process 4: Readers checking the final state
        ClientGetResponseMsg readFromP3 = (ClientGetResponseMsg) Patterns.ask(
                manager.getNodeById(nodeIds.get(2)), // Reader 3
                new ClientGetRequestMsg(targetKey),
                Duration.ofMillis(Constraints.TIMEOUT + 2000)).toCompletableFuture().get();

        ClientGetResponseMsg readFromP4 = (ClientGetResponseMsg) Patterns.ask(
                manager.getNodeById(nodeIds.get(0)), // Reader 4
                new ClientGetRequestMsg(targetKey),
                Duration.ofMillis(Constraints.TIMEOUT + 2000)).toCompletableFuture().get();

        // Assert 1: At least one of the writes must succeed (no deadlock or crash)
        assertTrue(resA.success() || resB.success(), "At least one write should succeed!");

        // Assert 2: Both readers must see the same final value, proving sequential
        // consistency
        assertEquals(readFromP3.data(), readFromP4.data(),
                "Sequential Consistency Violation: Process 3 and Process 4 see different values!");
    }

    @Test
    public void testReadDeniedDuringActiveWrite() throws Exception {
        // Arrange: Boot N nodes and write an initial value
        List<Integer> nodeIds = joinNodes(Constraints.N);
        int targetKey = 99;

        Patterns.ask(manager.getNodeById(nodeIds.get(0)),
                new ClientUpdateRequestMsg(targetKey, "V1"),
                Duration.ofSeconds(3)).toCompletableFuture().get();

        // Act 1: Send a Write request (this will lock the replicas)
        // Do not wait to finish
        CompletableFuture<Object> concurrentWrite = Patterns.ask(
                manager.getNodeById(nodeIds.get(0)),
                new ClientUpdateRequestMsg(targetKey, "V2"),
                Duration.ofMillis(Constraints.TIMEOUT + 2000))
                .toCompletableFuture();

        // Act 2: Immediately send a Read request while the write is holding the locks
        Thread.sleep(50); // Delay to make sure the write has started and acquired locks
        ClientGetResponseMsg readRes = (ClientGetResponseMsg) Patterns.ask(
                manager.getNodeById(nodeIds.get(1)),
                new ClientGetRequestMsg(targetKey),
                Duration.ofMillis(Constraints.TIMEOUT + 2000))
                .toCompletableFuture().get();

        // Assert: The Read must be denied (or timeout) because the keys are locked.
        // It should NOT return "V1" or "V2". It must return LOCK_DENIED_MSG or
        // TIMEOUT_MSG.
        assertTrue(
                readRes.data().equals(Errors.LOCK_DENIED_MSG) ||
                        readRes.data().equals(Errors.TIMEOUT_MSG),
                "Read should be denied while a Write is holding the locks!");

        // Wait for the write to finish 
        concurrentWrite.get();
    }
}
