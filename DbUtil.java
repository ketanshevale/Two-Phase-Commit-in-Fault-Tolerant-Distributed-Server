

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DbUtil {
	private static Connection connection = null;
	/**
	 * Get the connection uRL
	 * @return
	 */
    public static Connection getConnection() {
        if (connection != null)
            return connection;
        else {
            try {
            	// Read the properties
            	Properties prop = new Properties();
                //InputStream inputStream = DbUtil.class.getClassLoader().getResourceAsStream("/db.properties");
                Thread currentThread = Thread.currentThread();
                ClassLoader contextClassLoader = currentThread.getContextClassLoader();
                InputStream propertiesStream = contextClassLoader.getResourceAsStream("db.properties");
                if (propertiesStream != null) {
                	prop.load(propertiesStream);
                  // TODO close the stream
                } else {
                  // Properties file not found!
                }
                //prop.load(inputStream);
/*                driver=org.sqlite.JDBC
                		url=jdbc:sqlite:twophasecommitprotocol.db
*/
                //load the driver url username and password
                String driver = "org.sqlite.JDBC";
                String url = "jdbc:sqlite:twophasecommitprotocol.db";
                //System.out.println(driver+"   ----------- "+url);
                
                Class.forName(driver);
                connection = DriverManager.getConnection(url);
                // Perform the connection
                //System.out.println(connection.getClientInfo());
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (SQLException e) {
                e.printStackTrace();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return connection;
        }

    }
}
