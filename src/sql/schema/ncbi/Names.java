package sql.schema.ncbi;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import sql.queries.DbConnection;
import sql.schema.SchemableOM;

/**
 * NCBI names schema
 *
 */
public class Names implements SchemableOM {
	private JsonArray arr;
	private ResultSet rs;
	
	public Names() {
		arr = new JsonArray();
	}

	@Override
	public JsonArray retRes() throws SQLException {
		JsonObject jobj;
		ResultSetMetaData rsmeta = rs.getMetaData();

		while(rs.next()) {
			jobj = new JsonObject();
			for(int i = 1; i <= rsmeta.getColumnCount(); i++) {
				jobj.addProperty(rsmeta.getColumnLabel(i), rs.getString(i));
			}
			arr.add(jobj);
		}
		rs.close();

		return arr;
	}

	@Override
	public boolean hasRet(DbConnection gc, int id) throws SQLException {
		rs = gc.selStmt("names", new int[] {id});
		
		if(rs.isBeforeFirst()) {
			return true;
		}
		
		return false;
	}

}
