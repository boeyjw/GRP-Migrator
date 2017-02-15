package sql.gbif.schema;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import sql.queries.GbifConnection;

public class Distribution implements Schemable {
	private JsonArray arr;
	
	public Distribution() {
		arr = new JsonArray();
	}

	@Override
	public JsonArray retRes(GbifConnection gc, String coreID) {
		try {
			JsonObject jobj;
			ResultSet rs = gc.select("gd.source,gd.threatStatus,gd.locality,gd.lifeStage,gd.occuranceStatus,gd.locationID,gd.locationRemarks,gd.establishmentMeans,gd.countryCode,gd.country", "gbif_taxon gt inner join gbif_distribution gd on gt.coreID=gd.coreID", "gd.coreID=".concat(coreID));
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
