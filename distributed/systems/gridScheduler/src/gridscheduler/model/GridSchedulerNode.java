package gridscheduler.model;

import core.IMessageReceivedHandler;
import core.Message;
import core.SynchronizedSocket;
import example.LocalSocket;
import org.apache.log4j.*;

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
	private ConcurrentLinkedQueue<Job> jobQueue = null;
	
	// local address
	private final String address;

	// communications syncSocket
	private SynchronizedSocket syncSocket = null;
	
	// a hashmap linking each resource manager to an estimated load
	private ConcurrentHashMap<String, Integer> resourceManagersLoad = null;

	// the average load of all clusters connected to this GS node
	private int averageLoad = 0;

	private boolean jobReplicationEnabled = false;


	// polling frequency, 1hz
	private long pollSleep = 100;//1000

	// toggle for indicating the primary/replica GS node
	private boolean isReplica;

	private GridSchedulerNode replica = null;
	
	// polling thread
	private Thread pollingThread;
	private boolean running;



	private final static Logger logger = Logger.getLogger(GridSchedulerNode.class.getName());
	
	/**
	 * Constructs a new REPLICA GridSchedulerNode object at a given address.
	 * <p>
	 * <DL>
	 * <DT><B>Preconditions:</B>
	 * <DD>parameter <CODE>address</CODE> cannot be null
	 * </DL>
	 * @param address the gridscheduler's address to register at
	 */
	public GridSchedulerNode(String address, boolean jobReplicationEnabled) {
		// preconditions
		assert(address != null) : "parameter 'address' cannot be null";

		logger.warn("GridSchedulerNode " + address + " created.");

		//
		isReplica = true;

		this.jobReplicationEnabled = jobReplicationEnabled;

		// init members
		this.address = address;



		// start the polling thread
		running =  !isReplica;
		pollingThread = new Thread(this);
		pollingThread.start();

	}
	/**
	 * Constructs a new PRIMARY GridSchedulerNode object at a given address.
	 * <p>
	 * <DL>
	 * <DT><B>Preconditions:</B>
	 * <DD>parameter <CODE>address</CODE> cannot be null
	 * </DL>
	 * @param address the gridscheduler's address to register at
	 */
	public GridSchedulerNode(String address, GridSchedulerNode replica, boolean jobReplicationEnabled) {
		// preconditions
		assert(address != null) : "parameter 'address' cannot be null";

		logger.warn("GridSchedulerNode " + address + " created.");

		// set this instance as PRIMARY GS node
		isReplica = false;
		this.replica = replica;

		// init members
		this.address = address;
		this.resourceManagersLoad = new ConcurrentHashMap<String, Integer>();
		this.jobReplicationEnabled = jobReplicationEnabled;
		this.jobQueue = new ConcurrentLinkedQueue<Job>();

		// create a messaging syncSocket
		LocalSocket lSocket = new LocalSocket();
		syncSocket = new SynchronizedSocket(lSocket);
		// register the syncSocket under the name of the gridscheduler.
		// In this way, messages can be sent between components by name.
		syncSocket.registerGridSchedulerAddress(address);
		syncSocket.addMessageReceivedHandler(this);


		replica.connectToReplica(this);


		// start the polling thread
		running = !isReplica;
		pollingThread = new Thread(this);
		pollingThread.start();

	}

	public void connectToReplica(GridSchedulerNode replica){
		this.replica = replica;
		this.jobQueue = replica.getJobQueue();
		// take the reference of sync socket from the replica
		this.syncSocket = replica.getSyncSocket();
	}

	public ConcurrentLinkedQueue<Job> getJobQueue() {
		return jobQueue;
	}

	public void setIsReplicaStatus(boolean status){
		this.isReplica = status;
	}

	public boolean getIsReplicaStatus(){
		return this.isReplica;
	}

	public void toggleStatus() {

		isReplica = !isReplica;
		running = !running;
		replica.isReplica = !replica.isReplica;
		replica.running = !replica.running;

		//replica.jobQueue = this.jobQueue;
		if (this.isReplica){
			replica.registerToSyncSocket(this);
			logger.warn("GS node " + replica.getAddress() + " became ACTIVE.");
		}else {
			this.registerToSyncSocket(replica);
			logger.warn("GS node " + this.getAddress() + " became ACTIVE.");
		}


	}

	public void registerToSyncSocket(GridSchedulerNode gsNode){
		this.syncSocket = gsNode.getSyncSocket();
		this.syncSocket.registerGridSchedulerAddress(this.address);
		this.syncSocket.addMessageReceivedHandler(this);
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
	public synchronized void onMessageReceived(Message message) {
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
			Job job = controlMessage.getJob();
			job.addClusterToVisited(this.getAddress());
			jobQueue.add(job);
		}
			
		// one of the resource managers responded to a load request from this GS node
		if (controlMessage.getType() == ControlMessageType.ReplyLoad) {
			logger.info("GS: " + controlMessage.getDestination() + " received the load of: " + controlMessage.getLoad() + "% from RM: " + controlMessage.getSource());
			resourceManagersLoad.put(controlMessage.getSource(), controlMessage.getLoad());
		}

		// one of the resource managers responded to a job request from this GS node
		if (controlMessage.getType() == ControlMessageType.ReplyJob){
			logger.info("GS: " + this.getAddress() + " received job " + controlMessage.getJob().getId() + " from RM: " + controlMessage.getSource());
			Job job = controlMessage.getJob();
			job.addClusterToVisited(this.getAddress());
			jobQueue.add(controlMessage.getJob());
		}

		// one of the clusters notified the GS that it completed a job
		if (controlMessage.getType() == ControlMessageType.NotifyJobCompletion){
			//syncSocket.sendMessage(controlMessage,"localhost://placeholder"); //TODO this will no longer be necessary since the RMs are already notified
			jobQueue.remove(controlMessage.getJob());
			//TODO broadcast to all other GS nodes

		}
			
		
	}

	// finds the least loaded resource manager and returns its address
	private String getLeastLoadedRM() {
		String ret = null; 
		int minLoad = Integer.MAX_VALUE;
		
		// loop over all resource managers, and pick the one with the lowest load
		for (String rmAddress : resourceManagersLoad.keySet()) {
			if (resourceManagersLoad.get(rmAddress) <= minLoad) {
				ret = rmAddress;
				minLoad = resourceManagersLoad.get(rmAddress);
			}
		}
		
		return ret;		
	}

	// finds the least loaded resource manager and returns its address
	private String getSecondLeastLoadedRM(String leastLoadedRM) {

		String ret = null;
		int minLoad = Integer.MAX_VALUE;

		// loop over all resource managers, and pick the one
		// with the lowest load that hasn't been already chosen
		for(String rmAddress : resourceManagersLoad.keySet()){
			if (resourceManagersLoad.get(rmAddress) <= minLoad && !rmAddress.equals(leastLoadedRM)) {
				ret = rmAddress;
				minLoad = resourceManagersLoad.get(rmAddress);
			}
		}

		return ret;
	}

	private void sendReplicatedJob(String target, Job job){

		if (target != null) {

			ControlMessage cMessage = new ControlMessage(ControlMessageType.AddJob);
			cMessage.setJob(job);
			cMessage.setSource(this.getAddress());
			cMessage.setDestination(target);

			syncSocket.sendMessage(cMessage, "localsocket://" + target);
			logger.info("[GridSchedulerNode] GS " + this.getAddress() + " sends job " + cMessage.getJob().getId() + " to RM: " + target);

			jobQueue.remove(job);

			// increase the estimated load of that RM by 1 (because we just added a job)
			int load = resourceManagersLoad.get(target);
			resourceManagersLoad.put(target, load + 1);

		}
	}

	public int getNumberOfNonReplicatedJobs(){
		int nonReplicatedLoad = 0;
		if(resourceManagersLoad != null) {
			for (String rmAddress : resourceManagersLoad.keySet()) {
				nonReplicatedLoad += resourceManagersLoad.get(rmAddress);
			}
		}
		return nonReplicatedLoad;
	}

	private int calculateAverageLoad(){
		int average = 0;

		for(String rmAddress : resourceManagersLoad.keySet()){
			average += resourceManagersLoad.get(rmAddress);
		}
		//average /= (resourceManagersLoad.size() > 0 ? resourceManagersLoad.size() : 1);
		if (resourceManagersLoad.size() == 0){
			return 0;
		}
		average /= resourceManagersLoad.size();
		return average;
	}

	public void sendJobRequest(String target){

		if (target != null) {

			ControlMessage cMessage = new ControlMessage(ControlMessageType.RequestJob);
			cMessage.setSource(this.getAddress());
			cMessage.setDestination(target);

			syncSocket.sendMessage(cMessage, "localsocket://" + target);
			logger.info("[GridSchedulerNode] GS " + this.getAddress() + " request job from RM: " + target);

		}
	}

	private void requestJobFromRMwithHigherThanAverageLoad(int average) {
		for(String rmAddress:resourceManagersLoad.keySet()){
			if(resourceManagersLoad.get(rmAddress) > average){
				sendJobRequest(rmAddress);
			}
		}

	}

	public Job getJobFromGsNodeJobQueue(){
		for (Job job:jobQueue){
			if (job.getStatus() == JobStatus.Waiting && !job.getIsReplicated()){
				jobQueue.remove(job);
				job.addClusterToVisited(this.getAddress());
				return job;
			}
		}
		return null;
	}

	public void addJob(Job job){
		this.jobQueue.add(job);
	}

	/**
	 * Polling thread runner. This thread polls each resource manager in turn to get its load,
	 * then offloads any job in the waiting queue to that resource manager
	 */
	public void run() {
		while (running) {
			// send a message to each resource manager, requesting its load
			for (String rmAdress : resourceManagersLoad.keySet()) {

				ControlMessage cMessage = new ControlMessage(ControlMessageType.RequestLoad);

				cMessage.setSource(this.getAddress());
				cMessage.setDestination(rmAdress);

				syncSocket.sendMessage(cMessage, "localsocket://" + rmAdress);
			}

			// TODO take the job from the RM that has a load higher than the average of all
			// RMs and dispach it to the RM that has a load lower than the average of all connected RMs
			averageLoad = calculateAverageLoad();

			requestJobFromRMwithHigherThanAverageLoad(averageLoad);


			//TODO verify that the RM can accept any more jobs


			// schedule waiting messages to the different clusters
			for (Job job : jobQueue) {

				String leastLoadedRM = getLeastLoadedRM();
				// check the load to be less than 100%
				if(resourceManagersLoad.get(leastLoadedRM) < 100) {
					sendReplicatedJob(leastLoadedRM, job);
				}
				if(jobReplicationEnabled) {
					// replicate the job on at least one more cluster simultaneously
					if (resourceManagersLoad.size() > 1) {
						String secondLeastLoadedRM = getSecondLeastLoadedRM(leastLoadedRM);
						// check the load to be less than 100%
						if (resourceManagersLoad.get(secondLeastLoadedRM) < 100) {
							sendReplicatedJob(secondLeastLoadedRM, job);
						}
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
