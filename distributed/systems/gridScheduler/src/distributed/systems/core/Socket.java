package distributed.systems.core;

import distributed.systems.gridscheduler.model.GridScheduler;

/**
 * Created by michelcojocaru on 23/11/2017.
 */
public class Socket {

    private GridScheduler gridScheduler = null;
    private String address = null;

    public Socket(){

    }

    public void addMessageReceivedHandler(GridScheduler gridScheduler) {
        this.gridScheduler = gridScheduler;
    }
/*
    public final GridScheduler getGridScheduler() {
        if (this.gridScheduler != null)
            return gridScheduler;
        return null;
    }
*/
    public void register(String address) {
        this.address = address;
    }
}
