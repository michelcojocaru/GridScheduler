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
	ReplyLoad,
	ReplyJob,
	ReplyNoOfNonReplicatedJobs,
	ReplyNonReplicatedJob,
	ReplyNoOfNonReplicatedJobsOnOneRM,


	// from GS to RM
	RequestLoad,
	RequestJob,
	RequestNoOfNonReplicatedJobs,
	RequestNonReplicatedJob,
	RequestNoOfNonReplicatedJobsOnOneRM,

	// both ways
	AddJob,

	//both ways
	NotifyJobCompletion


}
