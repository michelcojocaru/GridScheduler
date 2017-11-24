package distributed.systems.core;

import distributed.systems.example.LocalSocket;
import distributed.systems.gridscheduler.model.ControlMessage;
import distributed.systems.gridscheduler.model.ControlMessageType;
import distributed.systems.gridscheduler.model.GridScheduler;
import distributed.systems.gridscheduler.model.ResourceManager;

import java.util.ArrayList;

public class SynchronizedSocket extends Socket{

	LocalSocket localSocket = null;
	static ArrayList<ResourceManager> resourceManagers = new ArrayList<ResourceManager>(); //good
	static GridScheduler gridScheduler = null;
	//TODO find a use for this name
	String socketName = null;


	public SynchronizedSocket(LocalSocket lSocket){
		this.localSocket = lSocket;
	}

	public void addMessageReceivedHandler(ResourceManager resourceManager) {
		this.resourceManagers.add(resourceManager);
		System.out.println("[Socket] RM: " + resourceManager.getName() + " registered to " + this.gridScheduler.getAddress());

	}

	public void addMessageReceivedHandler(GridScheduler gridScheduler) {
		this.gridScheduler = gridScheduler;
		System.out.println("[Socket] GS: " + gridScheduler.getAddress() + " registered to " + socketName);

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
					System.out.println("[Socket] GS: " + cMessage.getSource() + " is sending a message to RM: " + resourceManager.getName());
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
