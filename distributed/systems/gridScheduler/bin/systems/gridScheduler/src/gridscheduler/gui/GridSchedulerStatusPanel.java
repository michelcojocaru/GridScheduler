package gridscheduler.gui;

import gridscheduler.model.GridScheduler;

import java.awt.*;

/**
 * 
 * A panel that displays information about a Cluster.
 * 
 * @author Niels Brouwers, Boaz Pat-El
 *
 */
public class GridSchedulerStatusPanel extends StatusPanel {
	/**
	 * Generated serialversionUID
	 */
	private static final long serialVersionUID = -4375781364684663377L;

	private final static int padding = 4;
	private final static int fontHeight = 12;
	
	private final static int panelWidth = 300;
	private int colWidth = panelWidth / 2;

	private GridScheduler scheduler;
	
	public GridSchedulerStatusPanel(GridScheduler scheduler) {
		this.scheduler = scheduler;
		setPreferredSize(new Dimension(panelWidth,50));
	}
	
    protected void paintComponent(Graphics g) {
		// Let UI delegate paint first 
	    // (including background filling, if I'm opaque)
	    super.paintComponent(g);
	    
	    g.drawRect(0, 0, getWidth() - 1, getHeight() - 1);
	    g.setColor(Color.YELLOW);
	    g.fillRect(1, 1, getWidth() - 2, getHeight() - 2);
	    g.setColor(Color.BLACK);
	    
	    // draw the cluster name and load 
	    int x = padding;
	    int y = padding + fontHeight;
	    
	    g.drawString("Scheduler name ", x, y);
	    g.drawString("" + scheduler.getAddress(), x + colWidth, y);
	    y += fontHeight;
	    
	    g.drawString("Jobs waiting ", x, y);
	    g.drawString("" + scheduler.getWaitingJobs(), x + colWidth, y);
	    y += fontHeight;


    }	

}
