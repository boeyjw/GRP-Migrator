package sql.schema.music;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import sql.queries.DbConnection;
import sql.schema.SchemableOM;

public class CD implements SchemableOM {
	private SchemableOM subqueryOM;
	private DbConnection gc;
	
	private JsonArray arr;
	private ResultSet rs;

	public CD() {
		arr = new JsonArray();
	}
	
	@Override
	public JsonArray retRes() throws SQLException {
		JsonObject jobj;
		ResultSetMetaData rsmeta = rs.getMetaData();

		while(rs.next()) {
			jobj = new JsonObject();
			int i = 1;
			int cdID = rs.getInt(i++);
			for( ; i <= rsmeta.getColumnCount(); i++) {
				jobj.addProperty(rsmeta.getColumnLabel(i), rs.getString(i));
			}
			subqueryOM = new Track();
			if(subqueryOM.hasRet(gc, cdID))
				jobj.add("track", subqueryOM.retRes());
			arr.add(jobj);
		}
		rs.close();
		
		return arr;
	}

	@Override
	public boolean hasRet(DbConnection gc, int id) throws SQLException {
		rs = gc.selStmt("cd", new int[] {id});
		this.gc = gc;
		if(rs.isBeforeFirst()) {
			return true;
		}
		
		return false;
	}

}
