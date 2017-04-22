package sql.queries;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

/**
 * 
 * Handles connection credential. Supports only 1 connection.
 *
 */
public class DbConnection {
	private MysqlDataSource dataSource;
	private Connection connection;
	private HashMap<String, PreparedStatement> statement;

	public DbConnection(String servername, String user, String password) {
		statement = new HashMap<String, PreparedStatement>(5);
		dataSource = new MysqlDataSource();
		dataSource.setServerName(servername);
		dataSource.setUser(user);
		dataSource.setPassword(password);
	}

	/**
	 * Initialise database credentials with localhost
	 * @param servername The server in which its at
	 * @param dbName The database name in the server
	 * @param user Username associated with the server
	 * @param password Password associated with the username of the server
	 */
	public DbConnection(String servername, String dbName, String user, String password) {
		this(servername, user, password);
		if(dbName != null) {
			dataSource.setDatabaseName(dbName);
		}
	}
	
	/**
	 * Initialises database credentials with remote server.
	 * @param servername Remote server access point
	 * @param port Access point port
	 * @param dbName The database to be accessed
	 * @param user Username associated with the remote server
	 * @param password Password associated with the username of the remote server
	 */
	public DbConnection(String servername, int port, String dbName, String user, String password) {
		this(servername, dbName, user, password);
		dataSource.setPort(port);
	}
	
	/**
	 * Opens a connection with the server.
	 * @return The connection object.
	 */
	public Connection open() {
		try {
			connection = dataSource.getConnection();
		} catch (SQLException sqle) {
			System.err.println(sqle.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
		}

		return connection;
	}

	/**
	 * Optional re-opening of the connection after closing it.
	 * @param user Username associated with the server
	 * @param password Password associated with the username of the server
	 */
	public Connection open(String user, String password) throws SQLException {

		connection = dataSource.getConnection(user, password);

		return connection;
	}

	/**
	 * Closes the connection. No further operation can be done after this.
	 */
	public void close() {
		try {
			if(!connection.isClosed()) {
				connection.close();
			}
		} catch (SQLException sqle) {
			System.err.println(sqle.getMessage());
		}
	}

	/**
	 * Adds  a prepared statement into the map.
	 * @param purpose The key of the map, also describes what the query is for in one word
	 * @param query The prepared statement, which is an SQL query
	 * @return True if successfully added into the map. False otherwise.
	 */
	public boolean addPrepStmt(String purpose, String query) {
		try {
			statement.put(purpose.toLowerCase(), connection.prepareStatement(query));
			return true;
		} catch (SQLException sqle) {
			System.err.println(sqle.getMessage());
		}

		return false;
	}

	/**
	 * Selects a prepared statement from the map and returns its results.
	 * @param purpose The key of the map which is mapped to the prepared statement
	 * @param param The '?' parameters in the prepared statement, added according to the array index + 1
	 * @return The ResultSet of the query. Null otherwise.
	 */
	public ResultSet selStmt(String purpose, int[] param) {
		try {
			if(param != null) {
				for(int i = 1; i <= param.length; i++) {
					statement.get(purpose.toLowerCase()).setInt(i, param[i - 1]);
				}
			}

			return statement.get(purpose.toLowerCase()).executeQuery();
		} catch (SQLException sqle) {
			System.err.println(sqle.getMessage());
		}

		return null;
	}
}
