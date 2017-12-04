package gridscheduler.model;

import core.IMessageReceivedHandler;
import core.Message;
import core.SynchronizedSocket;
import example.LocalSocket;
import org.apache.log4j.Logger;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;


/**
 * 
 * The GridSchedulerNode class represents a single-server implementation of the grid scheduler in the
 * virtual grid system.
 * 
 * @author Niels Brouwers
 *
 */
public class GridSchedulerNode implements IMessageReceivedHandler, Runnable {


	// job queue
	private ConcurrentLinkedQueue<Job> jobQueue;
	
	// local address
	private final String address;

	// communications syncSocket
	private SynchronizedSocket syncSocket;
	
	// a hashmap linking each resource manager to an estimated load
	private ConcurrentHashMap<String, Integer> resourceManagersLoad;

	// polling frequency, 1hz
	private long pollSleep = 1000;
	
	// polling thread
	private Thread pollingThread;
	private boolean running;



	private final static Logger logger = Logger.getLogger(GridSchedulerNode.class.getName());
	
	/**
	 * Constructs a new GridSchedulerNode object at a given address.
	 * <p>
	 * <DL>
	 * <DT><B>Preconditions:</B>
	 * <DD>parameter <CODE>address</CODE> cannot be null
	 * </DL>
	 * @param address the gridscheduler's address to register at
	 */
	public GridSchedulerNode(String address) {
		// preconditions
		assert(address != null) : "parameter 'address' cannot be null";

		logger.warn("GridSchedulerNode " + address + " created.");

		// init members
		this.address = address;
		this.resourceManagersLoad = new ConcurrentHashMap<String, Integer>();
		this.jobQueue = new ConcurrentLinkedQueue<Job>();

		// create a messaging syncSocket
		LocalSocket lSocket = new LocalSocket();
		syncSocket = new SynchronizedSocket(lSocket);
		// register the syncSocket under the name of the gridscheduler.
		// In this way, messages can be sent between components by name.
		syncSocket.registerGridSchedulerAddress(address);
		syncSocket.addMessageReceivedHandler(this);


		// start the polling thread
		running = true;
		pollingThread = new Thread(this);
		pollingThread.start();

	}
	
	/**
	 * The gridscheduler's name also doubles as its URL in the local messaging system.
	 * It is passed to the constructor and cannot be changed afterwards.
	 * @return the name of the gridscheduler
	 */
	public String getAddress() {
		return address;
	}

	/**
	 * Gets the number of jobs that are waiting for completion.
	 * @return
	 */
	public int getWaitingJobs() {
		int ret = 0;
		ret = jobQueue.size();
		return ret;
	}

	public SynchronizedSocket getSyncSocket(){
		return this.syncSocket;
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
		
		// resource manager wants to join this grid scheduler 
		// when a new RM is added, its load is set to Integer.MAX_VALUE to make sure
		// no jobs are scheduled to it until we know the actual load
		if (controlMessage.getType() == ControlMessageType.ResourceManagerJoin) {
			resourceManagersLoad.put(controlMessage.getSource(), Integer.MAX_VALUE);
			logger.info("GS: " + controlMessage.getDestination() + " received a join request from RM: " + controlMessage.getSource());
		}
		// resource manager wants to offload a job to us
		if (controlMessage.getType() == ControlMessageType.AddJob) {
			//TODO log the GS also into the visited cluster
			logger.info("GS: " + this.getAddress() + " received job " + controlMessage.getJob().getId() + " from RM: " + controlMessage.getSource());
			jobQueue.add(controlMessage.getJob());
		}
			
		// resource manager wants to offload a job to us 		WHAT THE FUCK ? It means to get the load from the rm
		if (controlMessage.getType() == ControlMessageType.ReplyLoad) {
			logger.info("GS: " + controlMessage.getDestination() + " received the load of: " + controlMessage.getLoad() + "% from RM: " + controlMessage.getSource());
			resourceManagersLoad.put(controlMessage.getSource(), controlMessage.getLoad());
		}

		// one of the clusters notified the GS that it completed a job
		if (controlMessage.getType() == ControlMessageType.NotifyJobCompletion){
			syncSocket.sendMessage(controlMessage,"localhost://placeholder"); //TODO get rid of the address field
			jobQueue.remove(controlMessage.getJob());

		}
			
		
	}

	// finds the least loaded resource manager and returns its address
	private String getLeastLoadedRM() {
		String ret = null; 
		int minLoad = Integer.MAX_VALUE;
		
		// loop over all resource managers, and pick the one with the lowest load
		for (String key : resourceManagersLoad.keySet())
		{
			if (resourceManagersLoad.get(key) <= minLoad)
			{
				ret = key;
				minLoad = resourceManagersLoad.get(key);
			}
		}
		
		return ret;		
	}

	/**
	 * Polling thread runner. This thread polls each resource manager in turn to get its load,
	 * then offloads any job in the waiting queue to that resource manager
	 */
	public void run() {
		while (running) {
			// send a message to each resource manager, requesting its load
			for (String rmAdress : resourceManagersLoad.keySet())
			{
				ControlMessage cMessage = new ControlMessage(ControlMessageType.RequestLoad);

				//cMessage.setUrl(this.getAddress());
				cMessage.setSource(this.getAddress());
				cMessage.setDestination(rmAdress);

				syncSocket.sendMessage(cMessage, "localsocket://" + rmAdress);
			}

			// schedule waiting messages to the different clusters
			for (Job job : jobQueue)
			{
				// replicate the job on at least 2 clusters simultaneously
				for(int i = 0; i < 2; i++) {

					String leastLoadedRM = getLeastLoadedRM();

					if (leastLoadedRM != null) {

						ControlMessage cMessage = new ControlMessage(ControlMessageType.AddJob);
						cMessage.setJob(job);
						cMessage.setSource(this.getAddress());
						cMessage.setDestination(leastLoadedRM);

						syncSocket.sendMessage(cMessage, "localsocket://" + leastLoadedRM);
						logger.info("[GridSchedulerNode] GS " + this.getAddress() + " sends job " + cMessage.getJob().getId() + " to RM: " + leastLoadedRM);

						jobQueue.remove(job);

						// increase the estimated load of that RM by 1 (because we just added a job)
						int load = resourceManagersLoad.get(leastLoadedRM);
						resourceManagersLoad.put(leastLoadedRM, load + 1);

					}
				}
				
			}

			// sleep
			try
			{
				Thread.sleep(pollSleep);
			} catch (InterruptedException ex) {
				assert(false) : "Grid scheduler runtread was interrupted";
			}
			
		}
		
	}
	
	/**
	 * Stop the polling thread. This has to be called explicitly to make sure the program 
	 * terminates cleanly.
	 *
	 */
	public void stopPollThread() {
		running = false;
		try {
			pollingThread.join();
		} catch (InterruptedException ex) {
			assert(false) : "Grid scheduler stopPollThread was interrupted";
		}
		
	}

	public int getNumberOfConnectedRMs() {
		return resourceManagersLoad.size();
	}

}
