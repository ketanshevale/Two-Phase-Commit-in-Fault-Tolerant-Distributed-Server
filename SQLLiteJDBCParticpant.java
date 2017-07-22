import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class SQLLiteJDBCParticpant {
	public static Connection connection = null;

	public SQLLiteJDBCParticpant() {
		connection = DbUtil.getConnection();
	}

	public static void main(String[] args) {
		SQLLiteJDBCParticpant SQLLiteJDBCParticpant = new SQLLiteJDBCParticpant();
		Statement statement = null;
		try {
			statement = connection.createStatement();
			String sql = "CREATE TABLE LOGINFOPARTICIPANT " + "(TRANSACTION_INFO INT PRIMARY KEY     NOT NULL,"
					+ " FILENAME           TEXT    NOT NULL, " + " OPERATION_TYPE     TEXT    NOT NULL, "
					+ " STATUS        	 TEXT    NOT NULL, " + " CONTENT_FILE       TEXT    NOT NULL)";
			statement.executeUpdate(sql);
			statement.close();
		} catch (SQLException sqlException) {
			sqlException.printStackTrace();
		}
	}
}
