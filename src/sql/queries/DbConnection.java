package sql.queries;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

public class DbConnection {
	private MysqlDataSource ds;
	private Connection con;
	private Statement stmt;
	
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
	
	public ResultSet select(String query) {
		stmt = null;
		
		try {
			stmt = con.createStatement();
			return stmt.executeQuery(query);
		} catch (SQLException sqle) {
			sqle.getErrorCode();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return null;
	}
	
	public ResultSet select(String col, String table) {
		stmt = null;
		
		try {
			stmt = con.createStatement();
			return stmt.executeQuery("select " + col + " from " + table + ";");
		} catch (SQLException sqle) {
			sqle.getErrorCode();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return null;
	}
	
	public ResultSet select(String col, String table, String param) {
		stmt = null;
		
		try {
			stmt = con.createStatement();
			return stmt.executeQuery("select " + col + " from " + table + " where " + param + ";");
		} catch (SQLException sqle) {
			sqle.getErrorCode();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return null;
	}
	
	public ResultSet select(String col, String table, String orderby, String param) {
		stmt = null;
		
		try {
			stmt = con.createStatement();
			return stmt.executeQuery("select " + col + " from " + table + " order by " + orderby + " where " + param + ";");
		} catch (SQLException sqle) {
			sqle.getErrorCode();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return null;
	}
	
	public ResultSet select(String col, String table, String orderby, String param, String limit) {
		stmt = null;
		
		try {
			stmt = con.createStatement();
			return stmt.executeQuery("select " + col + " from " + table + " order by " + orderby + " where " + param + " limit " + limit + ";");
		} catch (SQLException sqle) {
			sqle.getErrorCode();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return null;
	}
	
	public ResultSet select(String col, String table, String orderby, String param, String limit, String offset) {
		stmt = null;
		
		try {
			stmt = con.createStatement();
			return stmt.executeQuery("select " + col + " from " + table + " order by " + orderby + " where " + param + " limit " + limit + " offset " + offset + ";");
		} catch (SQLException sqle) {
			sqle.getErrorCode();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return null;
	}
}
