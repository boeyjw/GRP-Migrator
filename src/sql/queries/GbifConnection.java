package sql.queries;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

public class GbifConnection {
	private MysqlDataSource ds;
	private Connection con;
	private DatabaseMetaData dmd;
	
	public GbifConnection() {
		try {
			ds = new MysqlDataSource();
			ds.setServerName("localhost");
			ds.setDatabaseName("ts-gbif");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void open() {
		try {
			con = ds.getConnection("root", "");
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
	
	public ResultSet select(String col, String table, String param) {
		ResultSet rs = null;
		Statement stmt = null;
		
		try {
			stmt = con.createStatement();
			if(param.equals("") || param.equals(null)) {
				rs = stmt.executeQuery("select " + col + " from " + table + ";");
			}
			else {
				rs = stmt.executeQuery("select " + col + " from " + table + " where " + param + ";");
			}
		} catch (SQLException sqle) {
			sqle.getErrorCode();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return rs;
	}
	
	public ResultSet getDmd() {
		try {
			dmd = con.getMetaData();
			return dmd.getTables(null, null, "%", null);
		} catch (SQLException sqle) {
			sqle.getErrorCode();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return null;
	}

}
