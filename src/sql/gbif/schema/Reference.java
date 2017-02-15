package sql.gbif.schema;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import sql.queries.GbifConnection;

public class Reference implements Schemable {
	private JsonArray arr;
	
	public Reference() {
		arr = new JsonArray();
	}

	@Override
	public JsonArray retRes(GbifConnection gc, String coreID) {
		try {
			JsonObject jobj;
			ResultSet rs = gc.select("gr.bibliographicCitation,gr.references,gr.source,gr.identifier", "gbif_taxon gt inner join gbif_reference gr on gt.coreID=gr.coreID", "gr.coreID=".concat(coreID));
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
