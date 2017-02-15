package sql.gbif.schema;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import sql.queries.GbifConnection;

public class VernacularName implements Schemable {
	private JsonArray arr;
	
	public VernacularName() {
		arr = new JsonArray();
	}

	@Override
	public JsonArray retRes(GbifConnection gc, String coreID) {
		try {
			JsonObject jobj;
			ResultSet rs = gc.select("gv.vernacularName,gv.source,gv.sex,gv.lifeStage,gv.language,gv.countryCode,gv.country", "gbif_taxon gt inner join gbif_vernacularname gv on gt.coreID=gv.coreID", "gv.coreID=".concat(coreID));
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
