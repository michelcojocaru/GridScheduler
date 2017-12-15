package gridscheduler.model;

import org.apache.log4j.Logger;

import java.util.ArrayList;



/**
 * This class represents a job that can be executed on a grid. 
 * 
 * @author Niels Brouwers
 *
 */
public class Job {
	private long duration;
	private JobStatus status;
	private long id;
	private ArrayList<String> visitedClusters = null;
	private boolean isReplicated;

	private final static Logger logger = Logger.getLogger(Job.class.getName());

	//Experiment data
	private long submit_time; //creation time + add to queue
	private long wait_time;
	private long run_time;

	/**
	 * Constructs a new Job object with a certain duration and id. The id has to be unique
	 * within the distributed system to avoid collisions.
	 * <P>
	 * <DL>
	 * <DT><B>Preconditions:</B>
	 * <DD>parameter <CODE>duration</CODE> should be positive
	 * </DL> 
	 * @param duration job duration in milliseconds 
	 * @param id job ID
	 */
	public Job(long duration, long id) {
		// Preconditions
		assert(duration > 0) : "parameter 'duration' should be > 0";

		this.duration = duration;
		this.status = JobStatus.Waiting;
		this.id = id;
		this.visitedClusters = new ArrayList<>();
		this.isReplicated = false;

		//Experimentals
		this.submit_time = System.nanoTime();
		this.wait_time = 0;
		this.run_time = 0;
	}

	public void addClusterToVisited(String cluster){
		visitedClusters.add(cluster);
		//logger.info("Cluster: " + cluster + " was added to Job " + this.getId() + " visited queue.");
	}

	public void removeClusterFromVisited(String cluster){
		if(visitedClusters.contains(cluster)){
			visitedClusters.remove(cluster);
			//logger.info("Cluster: " + cluster + " was removed from Jobs " + this.getId() + " visited queue.");
		}
	}

	public void setSubmit_time() {
		this.submit_time = System.nanoTime() - this.submit_time;
	}

	public long getSubmit_time() { return this.submit_time;}

	public void setWait_time() {
		this.wait_time = System.nanoTime() - this.wait_time;
	}

	public long getWait_time() { return this.wait_time; }

	public void setRun_time() {
		this.run_time = System.currentTimeMillis() - this.run_time;
	}

	public long getRun_time() { return this.run_time; }
	/**
	 * Returns the duration of this job. 
	 * @return the total duration of this job
	 */
	public double getDuration() {
		return duration;
	}

	/**
	 * Returns the status of this job.
	 * @return the status of this job
	 */
	public JobStatus getStatus() {
		return status;
	}

	/**
	 * Sets the status of this job.
	 * @param status the new status of this job
	 */
	public void setStatus(JobStatus status) {
		this.status = status;
	}

	/**
	 * The message ID is a unique identifier for a message. 
	 * @return the message ID
	 */
	public long getId() {
		return id;
	}

	/**
	 * @return a string representation of this job object
	 */
	public String toString() {
		return "Job {ID = " + id + "}";
	}

	public boolean getIsReplicated() {
		return this.isReplicated;
	}

	public void setIsReplicated(boolean isReplicated){
		this.isReplicated = isReplicated;
	}
}
