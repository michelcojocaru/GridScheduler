package gridscheduler;

import gridscheduler.gui.ClusterStatusPanel;
import gridscheduler.gui.GridSchedulerPanel;
import gridscheduler.model.Cluster;
import gridscheduler.model.GridScheduler;
import gridscheduler.model.Job;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import javax.swing.*;
import java.awt.event.KeyEvent;
import java.awt.event.KeyListener;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.concurrent.ThreadLocalRandom;


/**
 *
 * The Simulation class is an example of a grid computation scenario. Every 100 milliseconds 
 * a new job is added to first cluster. As this cluster is swarmed with jobs, it offloads
 * some of them to the grid scheduler, which in turn passes them to the other clusters.
 * 
 * @author Niels Brouwers, Boaz Pat-El
 */
public class Simulation implements Runnable,KeyListener {
	// Number of clusters in the simulation
	private final static int nrClusters = 5;

	// Number of nodes per cluster in the simulation
	private final static int nrNodes = 12;

	// Simulation components
	Cluster clusters[];

	GridSchedulerPanel gridSchedulerPanel;

	private static long jobCreationRatio = 200L;
	private static long jobDuration = 8000L;


	private final static Logger logger = Logger.getLogger(Simulation.class.getName());
	private static DecimalFormat df2 = new DecimalFormat(".##");






	/**
	 * Constructs a new simulation object. Study this code to see how to set up your own
	 * simulation.
	 */
	public Simulation() throws IOException {

		BasicConfigurator.configure();

		GridScheduler scheduler;

		// Setup the model. Create a grid scheduler and a set of clusters.
		scheduler = new GridScheduler("scheduler1");

		// Create a new gridscheduler panel so we can monitor our components
		gridSchedulerPanel = new GridSchedulerPanel(scheduler);
		gridSchedulerPanel.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

		logger.info("Simulation started.");

		// Create the clusters and nods
		clusters = new Cluster[nrClusters];
		for (int i = 0; i < nrClusters; i++) {
			clusters[i] = new Cluster("cluster" + i, scheduler, nrNodes);

			// Now create a cluster status panel for each cluster inside this gridscheduler
			ClusterStatusPanel clusterReporter = new ClusterStatusPanel(clusters[i]);
			gridSchedulerPanel.addStatusPanel(clusterReporter);
		}

		// Open the gridscheduler panel
		gridSchedulerPanel.start();

		// Run the simulation
		Thread runThread = new Thread(this);
		runThread.run(); // This method only returns after the simulation has ended

		// Now perform the cleanup

		// Stop clusters
		for (Cluster cluster : clusters)
			cluster.stopPollThread();

		// Stop grid scheduler
		scheduler.stopPollThread();
	}

	/**
	 * The main run thread of the simulation. You can tweak or change this code to produce
	 * different simulation scenarios.
	 */
	public void run() {

		long jobId = 0;
		gridSchedulerPanel.addKeyListener(this);
		// Do not stop the simulation as long as the gridscheduler panel remains open
		while (gridSchedulerPanel.isVisible()) {
			// Add a new job to the system that take up random time
			Job job = new Job(jobDuration + (int) (Math.random() * 5000), jobId++);

			clusters[ThreadLocalRandom.current().nextInt(0, nrClusters)].getResourceManager().addJob(job);


			try {
				// Sleep a while before creating a new job
				Thread.sleep(jobCreationRatio);
			} catch (InterruptedException e) {
				assert (false) : "Simulation runtread was interrupted";
			}

		}

	}

	@Override
	public void keyTyped(KeyEvent e) {

	}

	@Override
	public void keyPressed(KeyEvent e) {

	}

	@Override
	public void keyReleased(KeyEvent e) {
		// on UP key pressed produce jobs faster
		if (e.getKeyCode() == KeyEvent.VK_UP ) {
			//precondition: the job creation ratio can't reach this threshold
			if(jobCreationRatio > 50) {
				jobCreationRatio -= 50;
			}
			logger.warn("Job creation ratio INCREASED to " + df2.format(1000/(double)jobCreationRatio) + " jobs/sec.");
		}
		// on DOWN key pressed produce jobs slower
		if (e.getKeyCode() == KeyEvent.VK_DOWN ) {
			jobCreationRatio += 50;
			logger.warn("Job creation ratio DECREASED to " + df2.format(1000/(double)jobCreationRatio) + " jobs/sec.");
		}
		// on LEFT key pressed decrease the job duration
		if (e.getKeyCode() == KeyEvent.VK_LEFT ) {

			//precondition: the job duration can't be less than 0,5 sec
			if(jobDuration > 500) {
				jobDuration -= 500;
			}
			logger.warn("Job duration DECREASED to " + df2.format((double) jobDuration/1000) + " sec.");
		}
		// on RIGHT key pressed increase the job duration
		if (e.getKeyCode() == KeyEvent.VK_RIGHT ) {
			jobDuration += 500;
			logger.warn("Job duration INCREASED to " + df2.format((double) jobDuration/1000)  + " sec.");
		}
	}

	/**
	 * Application entry point.
	 *
	 * @param args application parameters
	 */
	public static void main(String[] args) throws IOException {
		// Create and run the simulation
		new Simulation();
	}

}

