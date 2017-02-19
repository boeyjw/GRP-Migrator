package sql.schema.gbif;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import sql.queries.DbConnection;
import sql.schema.Schemable;

public class Multimedia implements Schemable {
	private JsonArray arr;
	
	
	public Multimedia() {
		arr = new JsonArray();
	}

	@Override
	public JsonArray retRes(DbConnection gc, int coreID) {
		try {
			JsonObject jobj;
			ResultSet rs = gc.selStmt("mult", new int[] {coreID});
			ResultSetMetaData rsmeta = rs.getMetaData();
			
			while(rs.next()) {
				jobj = new JsonObject();
				for(int i = 1; i <= rsmeta.getColumnCount(); i++) {
					jobj.addProperty(rsmeta.getColumnName(i), rs.getString(i));
				}
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