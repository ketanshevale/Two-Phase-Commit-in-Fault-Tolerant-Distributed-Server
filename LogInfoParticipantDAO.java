import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;


public class LogInfoParticipantDAO implements LogInfoParticipantDAOSupport {
	private Connection connection;

	public LogInfoParticipantDAO() {
		connection = DbUtil.getConnection();
	}

	@Override
	public List<LogInfoParticipant> findAllLogInfo() {
		List<LogInfoParticipant> logInfoParticipants = new CopyOnWriteArrayList<LogInfoParticipant>();
		Statement stmt = null;
		try {
		/*	String sql = "CREATE TABLE LOGINFOPARTICIPANT " + "(TRANSACTION_INFO INT PRIMARY KEY     NOT NULL,"
					+ " FILENAME           TEXT    NOT NULL, " + " OPERATION_TYPE     TEXT    NOT NULL, "
					+ " STATUS        	 TEXT    NOT NULL, " + " CONTENT_FILE       TEXT    NOT NULL)";
		*/
			connection.setAutoCommit(false);
			stmt = connection.createStatement();
			ResultSet  resultSet = stmt.executeQuery("SELECT * FROM LOGINFOPARTICIPANT");
			while(resultSet.next()) {
				LogInfoParticipant logInfoParticipant = new LogInfoParticipant();
				logInfoParticipant.setOperation_Type(resultSet.getString(3));
				logInfoParticipant.setFileName(resultSet.getString(2));
				logInfoParticipant.setStatus(resultSet.getString(4));
				logInfoParticipant.setContent_File(resultSet.getString(5));
				logInfoParticipant.setTransaction_Info(Integer.parseInt(resultSet.getString(1)));
				logInfoParticipants.add(logInfoParticipant);
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
		return logInfoParticipants;
	}

	@Override
	public void insertIntoParticpantLog(String operation_type, int transaction, String fileName, String status,String content) {
		try {
			connection.setAutoCommit(false);
			PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO LOGINFOPARTICIPANT(OPERATION_TYPE,FILENAME,STATUS,CONTENT_FILE,TRANSACTION_INFO) VALUES (?,?,?,?,?)");
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
			PreparedStatement preparedStatement = connection.prepareStatement("UPDATE LOGINFOPARTICIPANT SET STATUS=? WHERE FILENAME=? AND OPERATION_TYPE = ? AND TRANSACTION_INFO=?");
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
	public LogInfoParticipant findLogInfoByFilename(String fileName) {
		LogInfoParticipant logInfoParticipant = new LogInfoParticipant();
		PreparedStatement stmt = null;
		try {
			connection.setAutoCommit(false);
			stmt = connection.prepareStatement("SELECT * FROM LOGINFOPARTICIPANT WHERE FILENAME =?");
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
	public List<LogInfoParticipant> findLogInfoByStatus(String status) {
		PreparedStatement stmt = null;
		List<LogInfoParticipant> logInfoParticipants = new CopyOnWriteArrayList<LogInfoParticipant>();
		
		try {
			connection.setAutoCommit(false);
			stmt = connection.prepareStatement("SELECT * FROM LOGINFOPARTICIPANT WHERE STATUS =?");
			stmt.setString(1, status);
			ResultSet  resultSet = stmt.executeQuery();
			while(resultSet.next()) {
				LogInfoParticipant logInfoParticipant = new LogInfoParticipant();
				logInfoParticipant.setOperation_Type(resultSet.getString(3));
				logInfoParticipant.setFileName(resultSet.getString(2));
				logInfoParticipant.setStatus(resultSet.getString(4));
				logInfoParticipant.setContent_File(resultSet.getString(5));
				logInfoParticipant.setTransaction_Info(Integer.parseInt(resultSet.getString(1)));
				logInfoParticipants.add(logInfoParticipant);
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
		return logInfoParticipants;
	}

	@Override
	public LogInfoParticipant findLogInfoByFilenameandTransactionId(String fileName, int transaction_Id) {
		LogInfoParticipant logInfoCordinator = new LogInfoParticipant();
		PreparedStatement stmt = null;
		try {
			connection.setAutoCommit(false);
			stmt = connection.prepareStatement("SELECT * FROM LOGINFOPARTICIPANT WHERE FILENAME =? AND TRANSACTION_INFO=?");
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
	public LogInfoParticipant findStatusForConcurrency(String status_1, String status_2,String status_3,String filename) {
		LogInfoParticipant logInfoParticipant = new LogInfoParticipant();
		PreparedStatement stmt = null;
		try {
			connection.setAutoCommit(false);
			stmt = connection.prepareStatement("SELECT * FROM LOGINFOPARTICIPANT WHERE STATUS IN (?,?,?) AND FILENAME =?");
			stmt.setString(1, status_1);
			stmt.setString(2, status_2);
			stmt.setString(3, status_3);
			stmt.setString(4, filename);
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
	public List<LogInfoParticipant> findLogInfoByStatusandOperatioType(String status, String operationType) {
		PreparedStatement stmt = null;
		List<LogInfoParticipant> logInfoParticipants = new CopyOnWriteArrayList<LogInfoParticipant>();
		
		try {
			connection.setAutoCommit(false);
			stmt = connection.prepareStatement("SELECT * FROM LOGINFOPARTICIPANT WHERE STATUS =? AND OPERATION_TYPE=?");
			stmt.setString(1, status);
			stmt.setString(2, operationType);
			ResultSet  resultSet = stmt.executeQuery();
			while(resultSet.next()) {
				LogInfoParticipant logInfoParticipant = new LogInfoParticipant();
				logInfoParticipant.setOperation_Type(resultSet.getString(3));
				logInfoParticipant.setFileName(resultSet.getString(2));
				logInfoParticipant.setStatus(resultSet.getString(4));
				logInfoParticipant.setContent_File(resultSet.getString(5));
				logInfoParticipant.setTransaction_Info(Integer.parseInt(resultSet.getString(1)));
				logInfoParticipants.add(logInfoParticipant);
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
		return logInfoParticipants;
	}
	
}
