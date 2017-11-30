package gridscheduler.model;

import core.IMessageReceivedHandler;
import core.Message;
import core.Socket;
import core.SynchronizedSocket;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * This class represents a resource manager in the VGS. It is a component of a cluster, 
 * and schedulers jobs to nodes on behalf of that cluster. It will offload jobs to the grid
 * scheduler if it has more jobs waiting in the queue than a certain amount.
 * 
 * The <i>jobQueueSize</i> is a variable that indicates the cutoff point. If there are more
 * jobs waiting for completion (including the ones that are running at one of the nodes)
 * than this variable, jobs are sent to the grid scheduler instead. This variable is currently
 * defaulted to [number of nodes] + MAX_QUEUE_SIZE. This means there can be at most MAX_QUEUE_SIZE jobs waiting 
 * locally for completion. 
 * 
 * Of course, this scheme is totally open to revision.
 * 
 * @author Niels Brouwers, Boaz Pat-El
 *
 */
public class ResourceManager implements INodeEventHandler, IMessageReceivedHandler {

	private Cluster cluster;
	private Queue<Job> jobQueue;
	private String name;
	private int jobQueueSize;
	public static final int MAX_QUEUE_SIZE = 32;

	// Scheduler url
	private String gridSchedulerNodeURL = null;

	private Socket socket;
	private SynchronizedSocket syncSocket;

	private final static Logger logger = Logger.getLogger(Supervisor.class.getName());

	/**
	 * Constructs a new ResourceManager object.
	 * <P> 
	 * <DL>
	 * <DT><B>Preconditions:</B>
	 * <DD>the parameter <CODE>cluster</CODE> cannot be null
	 * </DL>
	 * @param cluster the cluster to which this resource manager belongs.
	 */
	public ResourceManager(Cluster cluster) throws IOException {
		// preconditions
		assert(cluster != null);

		this.jobQueue = new ConcurrentLinkedQueue<Job>();

		this.cluster = cluster;
		this.name = cluster.getName();

		// Number of jobs in the queue must be larger than the number of nodes, because
		// jobs are kept in queue until finished. The queue is a bit larger than the 
		// number of nodes for efficiency reasons - when there are only a few more jobs than
		// nodes we can assume a node will become available soon to handle that job.
		jobQueueSize = cluster.getNodeCount() + MAX_QUEUE_SIZE;

		socket = new Socket();
		socket.register(name);

		socket.addMessageReceivedHandler(this);
	}

	/**
	 * Add a job to the resource manager. If there is a free node in the cluster the job will be
	 * scheduled onto that Node immediately. If all nodes are busy the job will be put into a local
	 * queue. If the local queue is full, the job will be offloaded to the grid scheduler.
	 * <DL>
	 * <DT><B>Preconditions:</B>
	 * <DD>the parameter <CODE>job</CODE> cannot be null
	 * <DD>a grid scheduler url has to be set for this rm before calling this function (the RM has to be
	 * connected to a grid scheduler)
	 * </DL>
	 * @param job the Job to run
	 */
	public void addJob(Job job) {
		// check preconditions
		assert(job != null) : "the parameter 'job' cannot be null";
		assert(gridSchedulerNodeURL != null) : "No grid scheduler URL has been set for this resource manager";

		// if the jobqueue is full, offload the job to the grid scheduler
		if (jobQueue.size() >= jobQueueSize) {

			ControlMessage controlMessage = new ControlMessage(ControlMessageType.AddJob);
			//include the sender url into the message
			controlMessage.setSource(cluster.getName());
			controlMessage.setDestination(gridSchedulerNodeURL);
			//controlMessage.setUrl(socketURL);
			controlMessage.setJob(job);
			job.addClusterToVisited(this.cluster.getName());
			syncSocket.sendMessage(controlMessage, "localsocket://" + gridSchedulerNodeURL);


			// otherwise store it in the local queue
		} else {
			jobQueue.add(job);
			scheduleJobs();
		}

	}

	/**
	 * Tries to find a waiting job in the jobqueue.
	 * @return
	 */
	public Job getWaitingJob() {
		// find a waiting job
		for (Job job : jobQueue) 
			if (job.getStatus() == JobStatus.Waiting) 
				return job;

		// no waiting jobs found, return null
		return null;
	}

	/**
	 * Counts to find a waiting job in the jobqueue.
	 * @return
	 */
	public int getWaitingJobsCount() {
		int count = 0;
		// find a waiting job
		for (Job job : jobQueue)
			if (job.getStatus() == JobStatus.Waiting)
				count++;

		// no waiting jobs found, return null
		return count;
	}

	/**
	 * Tries to schedule jobs in the jobqueue to free nodes. 
	 */
	public void scheduleJobs() {
		// while there are jobs to do and we have nodes available, assign the jobs to the 
		// free nodes
		Node freeNode;
		Job waitingJob;

		while ( ((waitingJob = getWaitingJob()) != null) && ((freeNode = cluster.getFreeNode()) != null) ) {
			freeNode.startJob(waitingJob);
		}

	}

	/**
	 * Called when a job is finished
	 * <p>
	 * pre: parameter 'job' cannot be null
	 */
	public void jobDone(Job job) {
		// preconditions
		assert(job != null) : "parameter 'job' cannot be null";

		// job finished, remove it from our pool
		jobQueue.remove(job);


		//ask the GS to notify all the other RMs to remove the job from their queues (if present)
		ControlMessage nMessage = new ControlMessage(ControlMessageType.NotifyJobCompletion);
		nMessage.setSource(this.cluster.getName());
		nMessage.setDestination(getGridSchedulerNodeAddress());
		nMessage.setJob(job);

		logger.info("Job " + job.getId() + " done on " + this.cluster.getName());

		syncSocket.sendMessage(nMessage,"localsocket://" + getGridSchedulerNodeAddress());

	}

	/**
	 * @return the url of the grid scheduler this RM is connected to 
	 */
	public String getGridSchedulerNodeAddress() {
		return gridSchedulerNodeURL;
	}

	public String getName(){
		return name;
	}

	public void setName(String name){
		this.name = name;
	}

	public Queue<Job> getJobQueue(){
		return this.jobQueue;
	}

	/**
	 * Connect to a grid scheduler
	 * <p>
	 * pre: the parameter 'supervisorURL' must not be null
	 * @param supervisorURL
	 */
	public void connectToSupervisor(String supervisorURL) {

		// preconditions
		assert(supervisorURL != null) : "the parameter 'gridSchedulerNodeURL' cannot be null";

		this.gridSchedulerNodeURL = supervisorURL;

		ControlMessage message = new ControlMessage(ControlMessageType.ResourceManagerJoin);
		message.setSource(cluster.getName());
		message.setDestination(supervisorURL);	//redundant I know...

		socket.sendMessage(message);

	}


	/**
	 * Message received handler
	 * <p>
	 * pre: parameter 'message' should be of type ControlMessage 
	 * pre: parameter 'message' should not be null 
	 * @param message a message
	 */
	public void onMessageReceived(Message message) {
		// preconditions
		assert(message instanceof ControlMessage) : "parameter 'message' should be of type ControlMessage";
		assert(message != null) : "parameter 'message' cannot be null";

		ControlMessage controlMessage = (ControlMessage)message;

		// resource manager wants to offload a job to us 
		if (controlMessage.getType() == ControlMessageType.AddJob)
		{
			//mark this cluster as visited
			controlMessage.getJob().addClusterToVisited(this.cluster.getName());
			//include this job in the waiting queue of this cluster
			jobQueue.add(controlMessage.getJob());
			scheduleJobs();
		}

		// Grid scheduler asks for the load of this resource manager
		if (controlMessage.getType() == ControlMessageType.RequestLoad)
		{
			logger.info("RM: " + this.cluster.getName() + " received a request load from GS: " + controlMessage.getSource());

			ControlMessage replyMessage = new ControlMessage(ControlMessageType.ReplyLoad);

			replyMessage.setSource(cluster.getName());
			replyMessage.setDestination(controlMessage.getSource()); // send back to the issuer of message
			replyMessage.setLoad(jobQueue.size()); //cluster.countFreeNodes())

			syncSocket.sendMessage(replyMessage, "localsocket://" + controlMessage.getSource());

		}

		// Grid Scheduler asks this RM to remove a pending job from its queue
		if (controlMessage.getType() == ControlMessageType.NotifyJobCompletion){
			for(Job job:jobQueue){
				if(job.getId() == controlMessage.getJob().getId()){
					jobQueue.remove(job);
					logger.warn("RM: " + this.cluster.getName() + " removed job " + job.getId() + " from its queue.");
					break;
				}
			}
		}

	}


}
