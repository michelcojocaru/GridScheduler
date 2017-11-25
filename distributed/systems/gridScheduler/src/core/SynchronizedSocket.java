package core;

import example.LocalSocket;
import gridscheduler.model.ControlMessage;
import gridscheduler.model.ControlMessageType;
import gridscheduler.model.GridScheduler;
import gridscheduler.model.ResourceManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;

public class SynchronizedSocket extends Socket{

	private LocalSocket localSocket = null;
	private static ArrayList<ResourceManager> resourceManagers = new ArrayList<ResourceManager>(); //good
	private static GridScheduler gridScheduler = null;
	//TODO find a use for this name
	private String socketName = null;

	private final static Logger logger = Logger.getLogger(SynchronizedSocket.class.getName());

	public SynchronizedSocket(LocalSocket lSocket){
		this.localSocket = lSocket;
	}

	public void addMessageReceivedHandler(ResourceManager resourceManager) {
		resourceManagers.add(resourceManager);
		logger.info("RM: " + resourceManager.getName() + " registered to " + gridScheduler.getAddress());

	}

	public void addMessageReceivedHandler(GridScheduler gs) {
		gridScheduler = gs;
		logger.info("GS: " + gridScheduler.getAddress() + " registered to " + socketName);

	}

	public void register(String url){
		this.socketName = url;
	}

	private void send(ControlMessage cMessage,String address) {

		if (cMessage.getDestination().equals(gridScheduler.getAddress())) {
			gridScheduler.onMessageReceived(cMessage);
		}else {

			for (ResourceManager resourceManager : resourceManagers) {
				if (cMessage.getDestination().equals(resourceManager.getName())) {
					logger.info("GS: " + cMessage.getSource() + " is sending a message to RM: " + resourceManager.getName());
					resourceManager.onMessageReceived(cMessage);
					break;
				}
			}
		}
	}

	//TODO check for defects
	public void sendMessage(ControlMessage cMessage, String address){
		// RM to GS / GS to RM
		if(cMessage.getType() == ControlMessageType.AddJob){

			if(gridScheduler != null || !resourceManagers.isEmpty()){
				send(cMessage,address);
			}
		}

		if(cMessage.getType() == ControlMessageType.RequestLoad){

			//System.out.println("GS to RM - RequestLoad ");
			if(!resourceManagers.isEmpty()){
				send(cMessage,address);
			}
		}

		if(cMessage.getType() == ControlMessageType.ReplyLoad){

			//System.out.println("RM to GS - ReplyLoad");
			if(gridScheduler != null){
				gridScheduler.onMessageReceived(cMessage);
			}
		}

		if(cMessage.getType() == ControlMessageType.ResourceManagerJoin){

			//System.out.println("RM connected to GS - ResourceManagerJoin");
			send(cMessage,address);
		}

	}

}
