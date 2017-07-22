import java.util.List;

public interface LogInfoCordinatorDAOSupport {
	/**
	 * Find all the log information of the participant
	 * @return
	 */
	public List<LogInfoCordinator> findAllLogInfo();
	/**
	 * Insert in to the participant log information table
	 * @param operation_tpe
	 * @param transaction
	 * @param fileName
	 * @param status
	 * @param content
	 */
	public void insertIntoCordinatorLog(String operation_tpe, int transaction,String fileName, String status, String content);
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
	public LogInfoCordinator findLogInfoByFilename(String fileName);
	
	public LogInfoCordinator findLogInfoByFilenameandTransactionId(String fileName,int transaction_Id);
	
	public LogInfoCordinator getLatestTransactionInfo();
	
	public List<LogInfoCordinator> findLogInfoByStatus(String status);
}
