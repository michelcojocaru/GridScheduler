package core;

import example.LocalSocket;
import gridscheduler.model.ControlMessage;
import gridscheduler.model.ControlMessageType;
import gridscheduler.model.GridSchedulerNode;
import gridscheduler.model.ResourceManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;

public class SynchronizedSocket {

	private LocalSocket localSocket = null;
	private ArrayList<ResourceManager> resourceManagers = new ArrayList<ResourceManager>(); //good
	private GridSchedulerNode gridSchedulerNode = null;
	//TODO find a use for this name
	private String gridSchdulerNodeAddress = "Supervisor";

	private final static Logger logger = Logger.getLogger(SynchronizedSocket.class.getName());

	public SynchronizedSocket(LocalSocket lSocket){
		this.localSocket = lSocket;
	}

	public void addMessageReceivedHandler(ResourceManager resourceManager) {

		resourceManagers.add(resourceManager);

		if(gridSchedulerNode != null){
			ControlMessage joinRequestMessage = new ControlMessage(ControlMessageType.ResourceManagerJoin);
			joinRequestMessage.setSource(resourceManager.getName());
			joinRequestMessage.setDestination(gridSchdulerNodeAddress);

			gridSchedulerNode.onMessageReceived(joinRequestMessage);
		}

		logger.info("RM: " + resourceManager.getName() + " registered to " + gridSchdulerNodeAddress);//gridSchedulerNode.getAddress());
	}

	public void addMessageReceivedHandler(GridSchedulerNode gsNode) {
		gridSchedulerNode = gsNode;
		logger.info("GS node: " + gridSchedulerNode.getAddress() + " registered to Supervisor");

	}

	public String getGridSchdulerNodeAddress(){
		return this.gridSchdulerNodeAddress;
	}

	public void registerGridSchedulerAddress(String address) {
		this.gridSchdulerNodeAddress = address;
	}
/*
	public void register(String url){
		this.socketName = url;
	}
*/
	private void send(ControlMessage cMessage,String address) {

		if (cMessage.getDestination().equals(gridSchedulerNode.getAddress())) {
			gridSchedulerNode.onMessageReceived(cMessage);
		}else {

			for (ResourceManager resourceManager : resourceManagers) {
				if (cMessage.getDestination().equals(resourceManager.getName())) {
					logger.info("GS: " + cMessage.getSource() + " is sending a " + cMessage.getType() + " message to RM: " + resourceManager.getName());
					resourceManager.onMessageReceived(cMessage);
					break;
				}
			}
		}
	}
	// TODO rewrite this for the current architecture with multiple gs nodes
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

/*
			if(gridSchedulerNode != null || !resourceManagers.isEmpty()){
				send(cMessage,address);
			}
*/
			if(cMessage.getDestination().equals(gridSchedulerNode.getAddress())){
				gridSchedulerNode.onMessageReceived(cMessage);
			}else{
				for(ResourceManager resourceManager:resourceManagers){
					if(resourceManager.getName().equals(cMessage.getDestination())){
						resourceManager.onMessageReceived(cMessage);
						break;
					}
				}
			}
		}

		if(cMessage.getType() == ControlMessageType.RequestLoad){

			System.out.println("GS: " + cMessage.getSource() + " to RM " + cMessage.getDestination()+ "- RequestLoad: ");
/*
			if(!resourceManagers.isEmpty()){
				send(cMessage,address);
			}
*/
			for(ResourceManager resourceManager:resourceManagers){
				if(resourceManager.getName().equals(cMessage.getDestination())){
					resourceManager.onMessageReceived(cMessage);
					break;
				}
			}
		}

		if(cMessage.getType() == ControlMessageType.ReplyLoad){

			System.out.println("RM " + cMessage.getSource() + "to GS " + cMessage.getDestination() + "- ReplyLoad");
			if(gridSchedulerNode != null){
				gridSchedulerNode.onMessageReceived(cMessage);
			}
		}

		if(cMessage.getType() == ControlMessageType.ResourceManagerJoin){

			System.out.println("RM " + cMessage.getSource() + "connected to GS " + cMessage.getDestination() + "- ResourceManagerJoin");
			if(cMessage.getDestination().equals(gridSchedulerNode.getAddress())){
				gridSchedulerNode.onMessageReceived(cMessage);
			}
			// send(cMessage,address);
		}

		if(cMessage.getType() == ControlMessageType.NotifyJobCompletion){
			System.out.println("Notify Job Completion from " + cMessage.getSource() + " to " + cMessage.getDestination());
			//broadcastToAllRMs(cMessage);
		}

	}


	public int getNoOfConnectedRMs(){
		return resourceManagers.size();
	}
}
