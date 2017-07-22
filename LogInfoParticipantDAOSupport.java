import java.util.List;

public interface LogInfoParticipantDAOSupport {
	/**
	 * Find all the log information of the participant
	 * @return
	 */
	public List<LogInfoParticipant> findAllLogInfo();
	/**
	 * Insert in to the participant log information table
	 * @param operation_tpe
	 * @param transaction
	 * @param fileName
	 * @param status
	 * @param content
	 */
	public void insertIntoParticpantLog(String operation_tpe, int transaction,String fileName, String status, String content);
	/**
	 * Update the status of the log information participant
	 * @param filename
	 * @param status
	 */
	void updateStatus(String filename, int transaction, String opertion, String status);
	/**
	 * Find the log info by descibing the filename
	 * @param fileName
	 * @return
	 */
	public LogInfoParticipant findLogInfoByFilename(String fileName);
	
	public List<LogInfoParticipant> findLogInfoByStatus(String status);
	
	public List<LogInfoParticipant> findLogInfoByStatusandOperatioType(String status,String operationType);
	
	public LogInfoParticipant findLogInfoByFilenameandTransactionId(String fileName,int transaction_Id);
	
	public LogInfoParticipant findStatusForConcurrency(String status_1, String status_2, String status_3, String filename);
}
