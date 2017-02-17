package sql.queries;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

public class DbConnection {
	private MysqlDataSource ds;
	private Connection con;
	
	public DbConnection(String servername, String dbName, String user, String password) {
		try {
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
	
	public ResultSet select(String col, String table) {
		ResultSet rs = null;
		Statement stmt = null;
		
		try {
			stmt = con.createStatement();
			rs = stmt.executeQuery("select " + col + " from " + table + ";");
		} catch (SQLException sqle) {
			sqle.getErrorCode();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return rs;
	}
	
	public ResultSet select(String col, String table, String param) {
		ResultSet rs = null;
		Statement stmt = null;
		
		try {
			stmt = con.createStatement();
			rs = stmt.executeQuery("select " + col + " from " + table + " where " + param + ";");
		} catch (SQLException sqle) {
			sqle.getErrorCode();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return rs;
	}
	
	public ResultSet select(String col, String table, String orderby, String param) {
		ResultSet rs = null;
		Statement stmt = null;
		
		try {
			stmt = con.createStatement();
			rs = stmt.executeQuery("select " + col + " from " + table + " order by " + orderby + " where " + param + ";");
		} catch (SQLException sqle) {
			sqle.getErrorCode();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return rs;
	}
	
	public ResultSet select(String col, String table, String orderby, String param, String limit) {
		ResultSet rs = null;
		Statement stmt = null;
		
		try {
			stmt = con.createStatement();
			rs = stmt.executeQuery("select " + col + " from " + table + " order by " + orderby + " where " + param + " limit " + limit + ";");
		} catch (SQLException sqle) {
			sqle.getErrorCode();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return rs;
	}
	
	public ResultSet select(String col, String table, String orderby, String param, String limit, String offset) {
		ResultSet rs = null;
		Statement stmt = null;
		
		try {
			stmt = con.createStatement();
			rs = stmt.executeQuery("select " + col + " from " + table + " order by " + orderby + " where " + param + " limit " + limit + " offset " + offset + ";");
		} catch (SQLException sqle) {
			sqle.getErrorCode();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return rs;
	}
}
