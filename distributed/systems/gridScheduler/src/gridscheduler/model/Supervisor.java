package gridscheduler.model;

import core.IMessageReceivedHandler;
import core.Socket;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

public class Supervisor implements IMessageReceivedHandler, Runnable {

    // a hashmap linking each grid scheduler to an estimated load
    private ConcurrentHashMap<String, Integer> gridSchedulersLoad = null;

    //
    private ConcurrentHashMap<GridSchedulerNode,Integer> gridSchedulerNodeConnectedRMs = null;

    // list of all the managed grid scheduler nodes
    private ArrayList<GridSchedulerNode> gridSchedulerNodes = null;

    private int noOfGsGroups = 0;

    // name of this supervisor
    private String address = null;

    private static int minNoOfConnections = Integer.MAX_VALUE;

    /**
     * Constructor of supervisor named @param address, which creates @param noOfGsGroups
     * grid scheduler nodes.
     * It register itself to the global socket & populate a hashmap used for retaining
     * the number of jobs waiting in each node initially set to 0.
     */
    public Supervisor(String address, int noOfGsGroups, boolean jobReplicationEnabled){
        // preconditions
        assert(noOfGsGroups > 0): "Number of grid scheduler nodes must be positive!";
        assert(address != null): "Supervisor must have a name!";

        this.address = address;
        this.noOfGsGroups = noOfGsGroups;

        // initialize the grid scheduler nodes that this supervisor is coordinating
        gridSchedulerNodes = new ArrayList<>(2 * noOfGsGroups);
        for(int i = 0; i < noOfGsGroups; i++){
            GridSchedulerNode replica = new GridSchedulerNode("gridSchedulerNode" + (i + 1), jobReplicationEnabled);
            gridSchedulerNodes.add(new GridSchedulerNode("gridSchedulerNode" + i, replica, jobReplicationEnabled));
            gridSchedulerNodes.add(replica);
        }

        // register supervisor to the global socket
        Socket.addMessageReceivedHandler(this);
        //Socket.register(address); // possibly redundant

        // initialize the load of each grid scheduler node to 0 because none of the
        // grid scheduler nodes have waiting jobs already.
        gridSchedulersLoad = new ConcurrentHashMap<>();
        for(GridSchedulerNode gsNode:gridSchedulerNodes){
            gridSchedulersLoad.put(gsNode.getAddress(),0);
        }

        // initialize the number of connected RMs of each grid scheduler node to 0 because none of the
        // grid scheduler nodes have RMs connected already.
        gridSchedulerNodeConnectedRMs = new ConcurrentHashMap<>();
        for(GridSchedulerNode gsNode:gridSchedulerNodes){
            gridSchedulerNodeConnectedRMs.put(gsNode,0);
        }



    }

    private GridSchedulerNode getPrimaryGsNode(int index){

        if(!gridSchedulerNodes.get(index * 2).getIsReplicaStatus()){
            return gridSchedulerNodes.get(index * 2);
        }
        return gridSchedulerNodes.get( (index * 2) + 1);
    }

    @Override
    public void run() {
        /*
        while(true){
            synchronized (this) {
                for (int i = 0; i < noOfGsGroups; i++) {
                    GridSchedulerNode primaryGsNode1 = getPrimaryGsNode(i);
                    GridSchedulerNode primaryGsNode2 = getPrimaryGsNode((i + 1) % noOfGsGroups);

                    evenLoadsOfGsNodes(primaryGsNode1, primaryGsNode2);
                }
            }
        }
        */
    }

    private void evenLoadsOfGsNodes(GridSchedulerNode primaryGsNode1, GridSchedulerNode primaryGsNode2) {
        int numberOfNonReplicatedJobsOnGSnode1 = primaryGsNode1.getNoOfNonReplicatedJobs();
        int numberOfNonReplicatedJobsOnGSnode2 = primaryGsNode2.getNoOfNonReplicatedJobs();

        int diff = Math.abs(numberOfNonReplicatedJobsOnGSnode1 - numberOfNonReplicatedJobsOnGSnode2);

        if (numberOfNonReplicatedJobsOnGSnode1 > numberOfNonReplicatedJobsOnGSnode2) {
            //TODO move diff no of jobs to primary2
            migrateNonReplicatedJobs(primaryGsNode1, primaryGsNode2, diff);
        }else if (numberOfNonReplicatedJobsOnGSnode1 < numberOfNonReplicatedJobsOnGSnode2) {
            //TODO move diff no of jobs to primary1
            migrateNonReplicatedJobs(primaryGsNode2, primaryGsNode1, diff);
        }
    }

    private void migrateNonReplicatedJobs(GridSchedulerNode primaryGsNodeSource, GridSchedulerNode primaryGsNodeDestination, int diff) {
        int remainingJobs = diff;
        while (remainingJobs > 0){

            primaryGsNodeSource.getHighestLoadedRMnumberOfNonReplicatedJobs();
            // busy waiting for the result
            while (primaryGsNodeSource.getNoOfNonReplicatedJobsOnOneRM() == -1) {}

            int availableNumberOfJobsToMove = primaryGsNodeSource.getNoOfNonReplicatedJobsOnOneRM();

            if (availableNumberOfJobsToMove > remainingJobs){
                migrateJobs(primaryGsNodeSource, primaryGsNodeDestination, remainingJobs);
            } else {
                migrateJobs(primaryGsNodeSource, primaryGsNodeDestination, availableNumberOfJobsToMove);
            }

            remainingJobs -= availableNumberOfJobsToMove;
        }

    }

    private void migrateJobs(GridSchedulerNode primaryGsNodeSource, GridSchedulerNode primaryGsNodeDestination, int remainingJobs) {

        for (int i = 0; i < remainingJobs; i++) {

            ControlMessage cMessage = new ControlMessage(ControlMessageType.RequestJob);
            cMessage.setSource(primaryGsNodeDestination.getAddress());
            cMessage.setDestination(primaryGsNodeSource.getHighestLoadedRMname());

            primaryGsNodeSource.getSyncSocket().sendMessage(cMessage,"localsocket://");
        }
    }

    /**
     * After a resource manager tried to connect to the supervisor through the global socket,
     * the global socket is instructing the supervisor to create a direct link between that
     * resource manager and the grid scheduler that has the least resource managers already
     * connected to it.
     * @param resourceManager the resource manager that tried to connect to a grid
     * scheduler node.
     */
    public void bindResourceManagerToGsNode(ResourceManager resourceManager){

        // get the grid scheduler node that has the least resource managers connected to it
        GridSchedulerNode targetGridSchedulerNode = getLeastLoadedGridSchedulerNode();

        // add +1 to the number of connected RMs of the target grid scheduler node
        int load = gridSchedulerNodeConnectedRMs.get(targetGridSchedulerNode);
        gridSchedulerNodeConnectedRMs.put(targetGridSchedulerNode, load + 1);

        //register one end of the synchronized socket to the resource manager
        resourceManager.setSyncSocket(targetGridSchedulerNode.getSyncSocket());

        // supervisor acts like a middle man and register a new resource manager
        // to the grid scheduler node that has the least other resource managers connected
        // to it order to balance the distribution.
        targetGridSchedulerNode.getSyncSocket().addMessageReceivedHandler(resourceManager);


    }

    /**
     * Each grid scheduler node has a number of resource managers connected to it.
     * @return leastLoadedGsNode the grid scheduler that has the least resource
     * managers connected.
     */
    public GridSchedulerNode getLeastLoadedGridSchedulerNode(){

        GridSchedulerNode leastLoadedGsNode = gridSchedulerNodes.get(0);

        for (GridSchedulerNode gsNode:gridSchedulerNodeConnectedRMs.keySet()) {
            if (gridSchedulerNodeConnectedRMs.get(gsNode) <= minNoOfConnections) {
                leastLoadedGsNode = gsNode;
                minNoOfConnections = gridSchedulerNodeConnectedRMs.get(gsNode);
            }
        }

        minNoOfConnections = gridSchedulerNodeConnectedRMs.get(leastLoadedGsNode);

        return leastLoadedGsNode;
    }

    /**
     * Returns the name of the supervisor.
     * @return address
     */
    public String getAddress() {
        return this.address;
    }

    /**
     * Returns the total number of jobs that are waiting in all the grid scheduler nodes
     * @return the String of noOfWaitingJobs.
     */
    //
    public String getWaitingJobs() {
        int noOfWaitingJobs = 0;
        for(GridSchedulerNode gsNode:gridSchedulerNodes){
            noOfWaitingJobs += gsNode.getWaitingJobs();
        }
        return String.valueOf(noOfWaitingJobs);
    }

    public void injectGSnodeFault(boolean status){
        //TODO reimplement this to make random faults in gs nodes
        gridSchedulerNodes.get(0).toggleStatus();
    }

    /**
     * Stop all the grid scheduler nodes that the supervisor is coordinating.
     */
    public void stopPollThread() {
        for(GridSchedulerNode gsNode:gridSchedulerNodes){
            gsNode.stopPollThread();
        }
    }
}
