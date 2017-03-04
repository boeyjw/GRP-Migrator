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
 * After all, this code is not meant for general purpose translation.
 *
 */
public class DbConnection {
	private MysqlDataSource ds;
	private Connection con;
	private HashMap<String, PreparedStatement> stmt;
	
	public DbConnection(String servername, String user, String password) {
		stmt = new HashMap<String, PreparedStatement>(5);
		ds = new MysqlDataSource();
		ds.setServerName(servername);
		ds.setUser(user);
		ds.setPassword(password);
	}
	
	/**
	 * Initialise database credentials then open a connection
	 * @param servername The server in which its at
	 * @param dbName The GBIF and NCBI database name in the server
	 * @param user Username associated with the server
	 * @param password Password associated with the username of the server
	 */
	public DbConnection(String servername, String dbName, String user, String password) {
		this(servername, user, password);
		if(dbName != null) {
			ds.setDatabaseName(dbName);
		}
	}
	
	public DbConnection(String servername, int port, String dbName, String user, String password) {
		this(servername, dbName, user, password);
		ds.setPort(port);
	}
	
	public void open() {
		try {
			con = ds.getConnection();
		} catch (SQLException sqle) {
			System.err.println(sqle.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Optional re-opening of the connection after closing it.
	 * @param user Username associated with the server
	 * @param password Password associated with the username of the server
	 */
	public void open(String user, String password) {
		try {
			con = ds.getConnection(user, password);
		} catch (SQLException sqle) {
			System.err.println(sqle.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Closes the connection. No further operation can be done after this.
	 */
	public void close() {
		try {
			if(!con.isClosed()) {
				con.close();
			}
		} catch (SQLException sqle) {
			System.err.println(sqle.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
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
			stmt.put(purpose, con.prepareStatement(query));
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
					stmt.get(purpose).setInt(i, param[i - 1]);
				}
			}
			
			return stmt.get(purpose).executeQuery();
		} catch (SQLException sqle) {
			System.err.println(sqle.getMessage());
		}
		
		return null;
	}
}
