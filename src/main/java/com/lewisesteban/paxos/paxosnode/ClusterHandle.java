package com.lewisesteban.paxos.paxosnode;

import com.lewisesteban.paxos.rpc.paxos.RemotePaxosNode;

import java.util.List;

public interface ClusterHandle {

    /**
     * @return Returns true is the cluster is properly initialized and connected, and this object is ready for calling
     * methods. Note : if isReady returns false, this object may contain incorrect information, and calling methods may
     * lead to unpredictable behavior.
     * The ClusterHandle will always be "ready" as soon as Paxos has started.
     */
    boolean isReady();

    /**
     * @return Unique identifier of the calling node.
     * This ID is the index of the current node in the list of nodes contained in the cluster.
     */
    int getMyNodeId();

    /**
     * Returns the ID of the fragment corresponding to this cluster.
     */
    int getFragmentId();

    /**
     * Returns the nodes contained in the cluster
     */
    List<RemotePaxosNode> getMembers();

    /**
     * Returns the number of nodes contained in the cluster
     */
    int getNbMembers();

    /**
     * @return ID of the leader (dedicated proposer), or null if there is none
     */
    Integer getLeaderNodeId();

    /**
     * If nodeId is null and no election is currently ongoing, an election will start
     */
    void setLeaderNodeId(Integer nodeId);
}
