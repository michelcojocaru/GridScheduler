package core;

import gridscheduler.model.ControlMessage;
import gridscheduler.model.GridSchedulerNode;
import gridscheduler.model.ResourceManager;
import gridscheduler.model.Supervisor;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by michelcojocaru on 23/11/2017.
 */
public class Socket {

    private ResourceManager resourceManager = null;
    private Supervisor supervisor = null;
    private String address = null;

    // hashmap that
    private ConcurrentHashMap<GridSchedulerNode, SynchronizedSocket> directConnections = new ConcurrentHashMap<>();

    public void addMessageReceivedHandler(Supervisor supervisor) {
        this.supervisor = supervisor;
    }

    public void addMessageReceivedHandler(ResourceManager resourceManager){
        this.resourceManager = resourceManager;
    }

    public ResourceManager getResourceManager(){
        if(this.resourceManager != null){
            return this.resourceManager;
        }
        return null;
    }

    public Supervisor getSupervisor(){
        if(this.supervisor != null){
            return this.supervisor;
        }
        return null;
    }

    public ConcurrentHashMap<GridSchedulerNode, SynchronizedSocket> getDirectConnections() {
        return directConnections;
    }

    /*
    public final Supervisor getGridScheduler() {
        if (this.supervisor != null)
            return supervisor;
        return null;
    }
*/

    public void sendMessage(ControlMessage cMessage){
        if (cMessage.getDestination().equals(supervisor.getAddress())) {
            supervisor.onMessageReceived(cMessage);
        }
    }

    public void register(String address) {
        this.address = address;
    }
}
