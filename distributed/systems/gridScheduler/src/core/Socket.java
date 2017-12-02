package core;

import gridscheduler.model.ResourceManager;
import gridscheduler.model.Supervisor;

import java.util.ArrayList;

/**
 * Created by michelcojocaru on 23/11/2017.
 */
public final class Socket {

    private static Supervisor supervisor = null;
    private static ArrayList<ResourceManager> resourceManagers = new ArrayList<>();

    // TODO possibly redundant
    private static String address = null;

    /**
     * Private constructor in order to mimic the functionality of
     * a top-level static class that is unavailable in Java.
     */
    private Socket(){

    }

    /**
     * Register the supervisor to the global socket.
     * @param spv
     */
    public static void addMessageReceivedHandler(Supervisor spv) {
        supervisor = spv;
    }

    /**
     * Register the @param resourceManager to the global socket.
     */
    public static void addMessageReceivedHandler(ResourceManager resourceManager){
        resourceManagers.add(resourceManager);
        if (supervisor != null){
            supervisor.bindResourceManagerToGsNode(resourceManager);
        }
    }
    // TODO erase this if unnecessary
    public static void removeMessageReceivedHandler(ResourceManager resourceManager){
        for(int i = 0; i < resourceManagers.size(); i++){
            // TODO check for reference equality vs content equality
            if(resourceManagers.get(i) == resourceManager){
                resourceManagers.remove(i);
            }
        }
    }

    /**
     * Clear the links of each resource manager to the supervisor since they
     * already have direct link to a grid scheduler node.
     */
    public static void unregisterAllResourceManagers(){
        resourceManagers.clear();
    }

    /**
     * Erase the link to the supervisor since each grid scheduler node already
     * has a link to a resource manager.
     */
    public static void unregisterSupervisor(){
        supervisor = null;
    }

    // TODO possibly redundant
    /*
    public static void register(String addr) {
        address = addr;
    }
    */
}
