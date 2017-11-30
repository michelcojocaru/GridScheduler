package core;

import example.LocalSocket;
import gridscheduler.model.*;
import org.apache.log4j.Logger;

import java.util.ArrayList;

public class SynchronizedSocket extends Socket{

	private LocalSocket localSocket = null;
	private static ArrayList<ResourceManager> resourceManagers = null; //good
	private static GridSchedulerNode gridSchedulerNode = null;

	//TODO find a use for this name
	private String socketName = null;

	private final static Logger logger = Logger.getLogger(SynchronizedSocket.class.getName());

	public SynchronizedSocket(LocalSocket lSocket){
		this.localSocket = lSocket;


	}

	public void addMessageReceivedHandler(ResourceManager resourceManager) {
		resourceManagers.add(resourceManager);
		logger.info("RM: " + resourceManager.getName() + " registered to " + gridSchedulerNode.getAddress());

	}

	public void addMessageReceivedHandler(GridSchedulerNode gs) {
		gridSchedulerNode = gs;
		logger.info("GS: " + gridSchedulerNode.getAddress() + " registered to " + socketName);

	}

	public void register(String url){
		this.socketName = url;
	}

	private void send(ControlMessage cMessage,String address) {

		if (cMessage.getDestination().equals(gridSchedulerNode.getAddress())) {
			gridSchedulerNode.onMessageReceived(cMessage);
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

	// broadcast the message to all RMs except the one that issued the request
	private void broadcastToAllRMs(ControlMessage cMessage){
		for (ResourceManager resourceManager : resourceManagers) {
			if (!cMessage.getSource().equals(resourceManager.getName())) {
				logger.info("GS: " + cMessage.getSource() + " is sending a job " + cMessage.getJob().getId() + " removal request to RM: " + resourceManager.getName());
				resourceManager.onMessageReceived(cMessage);
			}
		}
	}

	//TODO check for defects
	public void sendMessage(ControlMessage cMessage, String address){
		// RM to GS / GS to RM
		if(cMessage.getType() == ControlMessageType.AddJob){

			if(gridSchedulerNode != null || !resourceManagers.isEmpty()){
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
			if(gridSchedulerNode != null){
				gridSchedulerNode.onMessageReceived(cMessage);
			}
		}

		if(cMessage.getType() == ControlMessageType.ResourceManagerJoin){

			//System.out.println("RM connected to GS - ResourceManagerJoin");
			send(cMessage,address);
		}

		if(cMessage.getType() == ControlMessageType.NotifyJobCompletion){
			broadcastToAllRMs(cMessage);
		}

	}

}
