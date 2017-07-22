import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;


public class LogInfoCordinatorDAO implements LogInfoCordinatorDAOSupport {
	private Connection connection;

	public LogInfoCordinatorDAO() {
		connection = DbUtil.getConnection();
	}

	@Override
	public List<LogInfoCordinator> findAllLogInfo() {
		List<LogInfoCordinator> logInfoParticipants = new CopyOnWriteArrayList<LogInfoCordinator>();
		Statement stmt = null;
		try {
			connection.setAutoCommit(false);
			stmt = connection.createStatement();
			/*String sql = "CREATE TABLE LOGINFOCORDINATOR " + "(TRANSACTION_INFO INT PRIMARY KEY     NOT NULL,"
					+ " FILENAME           TEXT    NOT NULL, " + " OPERATION_TYPE     TEXT    NOT NULL, "
					+ " STATUS        	 TEXT    NOT NULL, " + " CONTENT_FILE       TEXT    NOT NULL)";
			*/
			ResultSet  resultSet = stmt.executeQuery("SELECT * FROM LOGINFOCORDINATOR");
			while(resultSet.next()) {
				LogInfoCordinator logInfoParticipant = new LogInfoCordinator();
				logInfoParticipant.setOperation_Type(resultSet.getString(3));
				logInfoParticipant.setFileName(resultSet.getString(2));
				logInfoParticipant.setStatus(resultSet.getString(4));
				logInfoParticipant.setContent_File(resultSet.getString(5));
				logInfoParticipant.setTransaction_Info(Integer.parseInt(resultSet.getString(1)));
				logInfoParticipants.add(logInfoParticipant);
			}
		} catch (SQLException e) {
			
		}
		try {
			stmt.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return logInfoParticipants;
	}

	@Override
	public void insertIntoCordinatorLog(String operation_type, int transaction, String fileName, String status,String content) {
		try {
			connection.setAutoCommit(false);
			PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO LOGINFOCORDINATOR(OPERATION_TYPE,FILENAME,STATUS,CONTENT_FILE,TRANSACTION_INFO) VALUES (?,?,?,?,?)");
			preparedStatement.setString(1, operation_type);
			preparedStatement.setString(2, fileName);
			preparedStatement.setString(3, status);
			preparedStatement.setString(4, content);
			preparedStatement.setInt(5, transaction);
			preparedStatement.executeUpdate();
			connection.commit();
			preparedStatement.close();
		} catch(SQLException e) {
			e.printStackTrace();
		}
		
	}

	@Override
	public void updateStatus(String filename, int transaction, String opertion, String status) {
		// TODO Auto-generated method stub
		try {
			connection.setAutoCommit(false);
			PreparedStatement preparedStatement = connection.prepareStatement("UPDATE LOGINFOCORDINATOR SET STATUS=? WHERE FILENAME=? AND OPERATION_TYPE =? AND TRANSACTION_INFO=?");
			preparedStatement.setString(1, status);
			preparedStatement.setString(2, filename);
			preparedStatement.setString(3, opertion);
			preparedStatement.setInt(4, transaction);
			preparedStatement.executeUpdate();
			connection.commit();
			preparedStatement.close();
		} catch(SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public LogInfoCordinator findLogInfoByFilename(String fileName) {
		LogInfoCordinator logInfoParticipant = new LogInfoCordinator();
		PreparedStatement stmt = null;
		try {
			connection.setAutoCommit(false);
			stmt = connection.prepareStatement("SELECT * FROM LOGINFOCORDINATOR WHERE FILENAME =?");
			stmt.setString(1, fileName);
			ResultSet  resultSet = stmt.executeQuery();
			while(resultSet.next()) {
				logInfoParticipant.setOperation_Type(resultSet.getString(3));
				logInfoParticipant.setFileName(resultSet.getString(2));
				logInfoParticipant.setStatus(resultSet.getString(4));
				logInfoParticipant.setContent_File(resultSet.getString(5));
				logInfoParticipant.setTransaction_Info(Integer.parseInt(resultSet.getString(1)));
			
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		try {
			stmt.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return logInfoParticipant;
	}
	
	@Override
	public LogInfoCordinator findLogInfoByFilenameandTransactionId(String fileName,int transaction_Id) {
		LogInfoCordinator logInfoCordinator = new LogInfoCordinator();
		PreparedStatement stmt = null;
		try {
			connection.setAutoCommit(false);
			stmt = connection.prepareStatement("SELECT * FROM LOGINFOCORDINATOR WHERE FILENAME =? AND TRANSACTION_INFO=?");
			stmt.setString(1, fileName);
			stmt.setInt(2, transaction_Id);
			ResultSet  resultSet = stmt.executeQuery();
			while(resultSet.next()) {
				logInfoCordinator.setOperation_Type(resultSet.getString(3));
				logInfoCordinator.setFileName(resultSet.getString(2));
				logInfoCordinator.setStatus(resultSet.getString(4));
				logInfoCordinator.setContent_File(resultSet.getString(5));
				logInfoCordinator.setTransaction_Info(Integer.parseInt(resultSet.getString(1)));
			
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		try {
			stmt.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return logInfoCordinator;
	}

	@Override
	public LogInfoCordinator getLatestTransactionInfo() {
		/*SELECT * FROM tablename ORDER BY column DESC LIMIT 1;*/
		LogInfoCordinator logInfoCordinator = new LogInfoCordinator();
		PreparedStatement stmt = null;
		try {
			connection.setAutoCommit(false);
			stmt = connection.prepareStatement("SELECT * FROM LOGINFOCORDINATOR ORDER BY TRANSACTION_INFO DESC LIMIT 1");
			ResultSet  resultSet = stmt.executeQuery();
			while(resultSet.next()) {
				logInfoCordinator.setOperation_Type(resultSet.getString(3));
				logInfoCordinator.setFileName(resultSet.getString(2));
				logInfoCordinator.setStatus(resultSet.getString(4));
				logInfoCordinator.setContent_File(resultSet.getString(5));
				logInfoCordinator.setTransaction_Info(Integer.parseInt(resultSet.getString(1)));
			
			}
			stmt.close();
		} catch (SQLException e) {
			logInfoCordinator.setOperation_Type("DUMMY");
			logInfoCordinator.setFileName("DUMMY");
			logInfoCordinator.setStatus("DUMMY");
			logInfoCordinator.setContent_File("DUMMY");
			logInfoCordinator.setTransaction_Info(0);
			return logInfoCordinator;
		}
		
		return logInfoCordinator;
	}

	@Override
	public List<LogInfoCordinator> findLogInfoByStatus(String status) {
		PreparedStatement stmt = null;
		List<LogInfoCordinator> LogInfoCordinators = new CopyOnWriteArrayList<LogInfoCordinator>();
		
		try {
			connection.setAutoCommit(false);
			stmt = connection.prepareStatement("SELECT * FROM LOGINFOCORDINATOR WHERE STATUS =?");
			stmt.setString(1, status);
			ResultSet  resultSet = stmt.executeQuery();
			while(resultSet.next()) {
				LogInfoCordinator logInfoCordinator = new LogInfoCordinator();
				logInfoCordinator.setOperation_Type(resultSet.getString(3));
				logInfoCordinator.setFileName(resultSet.getString(2));
				logInfoCordinator.setStatus(resultSet.getString(4));
				logInfoCordinator.setContent_File(resultSet.getString(5));
				logInfoCordinator.setTransaction_Info(Integer.parseInt(resultSet.getString(1)));
				LogInfoCordinators.add(logInfoCordinator);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		try {
			stmt.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return LogInfoCordinators;
	}
	
	
}
