package gridscheduler.model;

import core.IMessageReceivedHandler;
import core.Socket;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

public class Supervisor implements IMessageReceivedHandler, Runnable {

    // a hashmap linking each grid scheduler to an estimated load
    private ConcurrentHashMap<String, Integer> gridSchedulersLoad = null;

    // list of all the managed grid scheduler nodes
    private ArrayList<GridSchedulerNode> gridSchedulerNodes = null;

    // name of this supervisor
    private String address = null;

    /**
     * Constructor of supervisor named @param address, which creates @param noOfGsNodes
     * grid scheduler nodes.
     * It register itself to the global socket & populate a hashmap used for retaining
     * the number of jobs waiting in each node initially set to 0.
     */
    public Supervisor(String address, int noOfGsNodes){
        // preconditions
        assert(noOfGsNodes > 0): "Number of grid scheduler nodes must be positive!";
        assert(address != null): "Supervisor must have a name!";

        // initialize the grid scheduler nodes that this supervisor is coordinating
        gridSchedulerNodes = new ArrayList<>(noOfGsNodes);
        for(int i = 0; i < noOfGsNodes; i++){
            gridSchedulerNodes.add(new GridSchedulerNode("gridSchedulerNode" + i));
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



    }

    @Override
    public void run() {

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

        int minLoad = Integer.MAX_VALUE;
        GridSchedulerNode leastLoadedGsNode = null;

        for(GridSchedulerNode gsNode:gridSchedulerNodes){
            int load = gsNode.getNumberOfConnectedRMs();
            if (load < minLoad){
                leastLoadedGsNode = gsNode;
                minLoad = load;
            }
        }

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

    /**
     * Stop all the grid scheduler nodes that the supervisor is coordinating.
     */
    public void stopPollThread() {
        for(GridSchedulerNode gsNode:gridSchedulerNodes){
            gsNode.stopPollThread();
        }
    }
}
