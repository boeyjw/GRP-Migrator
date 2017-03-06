package sql.schema.ncbi;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import sql.queries.DbConnection;
import sql.schema.SchemableOM;

public class Citations implements SchemableOM {
	JsonArray arr;
	
	public Citations() {
		arr = new JsonArray();
	}

	@Override
	public JsonArray retRes(DbConnection gc, int id) {
		try {
			JsonObject jobj;
			ResultSet rs = gc.selStmt("cit", new int[] {id});
			ResultSetMetaData rsmeta = rs.getMetaData();
			
			while(rs.next()) {
				jobj = new JsonObject();
				int i = 1;
				
				jobj.addProperty(rsmeta.getColumnLabel(i), rs.getString(i++));
				jobj.addProperty(rsmeta.getColumnLabel(i), rs.getInt(i++));
				jobj.addProperty(rsmeta.getColumnLabel(i), rs.getInt(i++));
				jobj.addProperty(rsmeta.getColumnLabel(i), rs.getString(i++));
				jobj.addProperty(rsmeta.getColumnLabel(i), rs.getString(i));
				
				arr.add(jobj);
			}
			rs.close();
		} catch (SQLException sqle) {
			sqle.getErrorCode();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return arr;
	}

}
