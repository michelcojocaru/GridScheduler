package gridscheduler.model;

/**
 * 
 * Different types of control messages. Feel free to add new message types if you need any. 
 * 
 * @author Niels Brouwers
 *
 */
public enum ControlMessageType {

	// from RM to GS
	ResourceManagerJoin,

	// from RM to GS node
	ReplyLoad,

	// from GS node to RM
	RequestLoad,

	// both ways
	AddJob,

	//both ways
	NotifyJobCompletion

}
