package it.unitn;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

import it.unitn.constraints.Constraints;
import it.unitn.models.Messages.ClientUpdateRequestMsg;
import it.unitn.models.Messages.NodeStateReplyMsg;

public class NetworkTest extends SystemTestBase {

    @Test
    public void testNetworkBootstrapping() throws Exception {
        // Arrange: boot N nodes
        List<Integer> nodeIds = joinNodes(Constraints.N);

        // Act: Ask the first node for the network state
        NodeStateReplyMsg state = getNodeState(nodeIds.get(0));

        // Assert: The node should se exactly N nodes in the network
        assertNotNull(state);
        assertEquals(Constraints.N, state.networkView().size(),
                "Node should see exactly N active nodes in the ring");

        // Ensure that the node sees all the expected node IDs
        for (int nodeId : nodeIds) {
            assertTrue(state.networkView().containsKey(nodeId),
                    "Network view is missing node: " + nodeId);
        }

        // Storage should be empty
        assertTrue(state.storage().isEmpty(), "Storage should be empty on startup");
    }

    @Test
    public void testSingleNodeNetwork() throws Exception{
        // Arrange: Boot only 1 node (less than Constraints.N)
        List<Integer> nodeIds = joinNodes(1);
        int soleNodeId = nodeIds.get(0);

        // Act: Write data to the network
        int testKey = 31;
        manager.getNodeById(soleNodeId).tell(
            new ClientUpdateRequestMsg(testKey, "lonely_node_data"), 
            dummyClient
        );
        Thread.sleep(500);

        // Assert: The single node should store the data, even though it can't find N replicas
        NodeStateReplyMsg state = getNodeState(soleNodeId);

        assertNotNull(state.storage().get(testKey), 
                "Node must store the key locally even if N > current network size");
        assertEquals("lonely_node_data", state.storage().get(testKey).value());
        assertEquals(1, state.storage().get(testKey).version());
    }
}
