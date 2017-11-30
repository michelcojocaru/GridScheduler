package gridscheduler.model;

import core.IMessageReceivedHandler;
import core.Message;
import core.Socket;
import core.SynchronizedSocket;
import example.LocalSocket;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;


/**
 * 
 * The Supervisor class represents a single-server implementation of the grid scheduler in the
 * virtual grid system.
 * 
 * @author Niels Brouwers
 *
 */
public class Supervisor implements IMessageReceivedHandler{

	// hashmap that register the grid scheduler nodes
	private ConcurrentHashMap<String, Integer> gridSchedulerNodesLoad = null;



	private String address;

	// grid schduler nodes
	private ArrayList<GridSchedulerNode> gridSchedulerNodes = null;

	// communications socket
	private Socket socket;

	// logger
	private final static Logger logger = Logger.getLogger(GridSchedulerNode.class.getName());

	/**
	 * Constructs a new Supervisor object at a given address.
	 * <p>
	 * <DL>
	 * <DT><B>Preconditions:</B>
	 * <DD>parameter <CODE>address</CODE> cannot be null
	 * </DL>
	 * @param address the gridscheduler's address to register at
	 */
	public Supervisor(String address, int noOfGsNodes) {

		gridSchedulerNodesLoad = new ConcurrentHashMap<String, Integer>();

		this.address = address;

		// create a messaging socket
		//LocalSocket lSocket = new LocalSocket();
		socket = new Socket();
		socket.addMessageReceivedHandler(this);

		// register the socket under the name of the gridscheduler.
		// In this way, messages can be sent between components by name.
		socket.register(address);

		for(int i = 0;i < noOfGsNodes; i++){
			String name = "gsNode" + i;
			gridSchedulerNodes.add(new GridSchedulerNode(name));
			// the load of each node is 0 because it hasn't have any connected RMs
			gridSchedulerNodesLoad.put(name,0);
		}

	}

	/**
	 * Receives a message from another component.
	 * <p>
	 * <DL>
	 * <DT><B>Preconditions:</B>
	 * <DD>parameter <CODE>message</CODE> should be of type ControlMessage 
	 * <DD>parameter <CODE>message</CODE> should not be null
	 * </DL> 
	 * @param message a message
	 */
	public void onMessageReceived(Message message) {
		// preconditions
		assert(message instanceof ControlMessage) : "parameter 'message' should be of type ControlMessage";
		assert(message != null) : "parameter 'message' cannot be null";

		ControlMessage controlMessage = (ControlMessage) message;
		
		// resource manager wants to join this supervisor
		if (controlMessage.getType() == ControlMessageType.ResourceManagerJoin) {

			String leastLoadedGsNodeAddress  = getLeastLoadedGsNode();
			int load = gridSchedulerNodesLoad.get(leastLoadedGsNodeAddress);
			gridSchedulerNodesLoad.put(leastLoadedGsNodeAddress, load + 1);

			// create direct communication channel between GS node & RM
			LocalSocket lSocket = new LocalSocket();
			SynchronizedSocket syncSocket = new SynchronizedSocket(lSocket);

			// register to the direct socket the RM & GS node
			GridSchedulerNode targetGridSchedulerNode = getGridSchedulerByName(leastLoadedGsNodeAddress);
			syncSocket.addMessageReceivedHandler(targetGridSchedulerNode); // good
			syncSocket.addMessageReceivedHandler(socket.getResourceManager()); // good

			socket.getDirectConnections().put(targetGridSchedulerNode,syncSocket);

			syncSocket.sendMessage(controlMessage,"localsocket://");

			//TODO change this
			logger.info("GS: " + this.getAddress() + " received a join request from RM: " + controlMessage.getSource());
		} else {
			//TODO redirect message from RM to GS NODE X?




		}
			
		
	}

	public String getAddress(){
		return this.address;
	}

	private GridSchedulerNode getGridSchedulerByName(String address){

		for(GridSchedulerNode gsNode: gridSchedulerNodes){
			if(address.equals(gsNode.getAddress())){
				return gsNode;
			}

		}
		return null;
	}

	// finds the least loaded resource manager and returns its address
	private String getLeastLoadedGsNode() {
		String ret = null; 
		int minLoad = Integer.MAX_VALUE;
		
		// loop over all resource managers, and pick the one with the lowest load
		for (String key : gridSchedulerNodesLoad.keySet())
		{
			if (gridSchedulerNodesLoad.get(key) <= minLoad)
			{
				ret = key;
				minLoad = gridSchedulerNodesLoad.get(key);
			}
		}
		
		return ret;		
	}

	public int getWaitingJobs(){
		for(GridSchedulerNode gridSchedulerNode:gridSchedulerNodes){

		}
	}

	public void stopPollThread() {

		for(GridSchedulerNode gs: gridSchedulerNodes){
			gs.stopPollThread();
		}
	}


	

	
}
