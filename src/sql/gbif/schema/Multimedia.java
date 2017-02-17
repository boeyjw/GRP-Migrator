package sql.gbif.schema;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import sql.queries.DbConnection;

public class Multimedia implements Schemable {
	private JsonArray arr;
	
	
	public Multimedia() {
		arr = new JsonArray();
	}

	@Override
	public JsonArray retRes(DbConnection gc, String coreID) {
		try {
			JsonObject jobj;
			ResultSet rs = gc.select("gm.references,gm.description,gm.title,gm.contributor,gm.source,gm.created,gm.license,gm.identifier,gm.creator,gm.publisher,gm.rightsHolder", 
					"gbif_taxon gt inner join gbif_multimedia gm on gt.coreID=gm.coreID", "gm.coreID=".concat(coreID));
			ResultSetMetaData rsmeta = rs.getMetaData();
			
			while(rs.next()) {
				jobj = new JsonObject();
				for(int i = 1; i <= rsmeta.getColumnCount(); i++) {
					jobj.addProperty(rsmeta.getColumnName(i), rs.getString(i));
				}
				arr.add(jobj);
			}
		} catch (SQLException sqle) {
			sqle.getErrorCode();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return arr;
	}

}
