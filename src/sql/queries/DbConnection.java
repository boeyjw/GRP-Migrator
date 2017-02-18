package sql.queries;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

public class DbConnection {
	private MysqlDataSource ds;
	private Connection con;
	private HashMap<String, PreparedStatement> stmt;
	
	public DbConnection(String servername, String dbName, String user, String password) {
		try {
			stmt = new HashMap<String, PreparedStatement>(5);
			ds = new MysqlDataSource();
			ds.setServerName(servername);
			ds.setDatabaseName(dbName);
			open(user, password);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void open(String user, String password) {
		try {
			con = ds.getConnection(user, password);
		} catch (SQLException sqle) {
			sqle.getErrorCode();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void close() {
		try {
			if(!con.isClosed()) {
				con.close();
			}
		} catch (SQLException sqle) {
			sqle.getErrorCode();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public boolean addPrepStmt(String purpose, String query) {
		try {
			stmt.put(purpose, con.prepareStatement(query));
			return true;
		} catch (SQLException sqle) {
			sqle.getErrorCode();
		}
		
		return false;
	}
	
	public ResultSet selStmt(String purpose, int colIndex, int param, int lim) {
		try {
			if(colIndex > 0 && param > -1) {
				stmt.get(purpose).setInt(colIndex, param);
			}
			if(lim > 0) {
				stmt.get(purpose).setFetchSize(lim);
			}
			
			return stmt.get(purpose).executeQuery();
		} catch (SQLException sqle) {
			sqle.getErrorCode();
		}
		
		return null;
	}
}
