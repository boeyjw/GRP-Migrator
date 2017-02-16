package sql.tojson;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import sql.gbif.schema.Distribution;
import sql.gbif.schema.Multimedia;
import sql.gbif.schema.Reference;
import sql.gbif.schema.Schemable;
import sql.gbif.schema.VernacularName;
import sql.queries.GbifConnection;

public class Converter {
	private JsonArray gbif_master;
	private Schemable subquery;
	
	public Converter() {
		gbif_master = new JsonArray();
	}
	
	public JsonArray getgbifMaster() {
		return gbif_master;
	}
	
	public void makeTaxon(GbifConnection gc) {
		try {
			JsonObject gm_obj;
			ResultSet rs = gc.select("*", "gbif_taxon", null);
			ResultSetMetaData rsmeta = rs.getMetaData();
			
			while(rs.next()) {
				gm_obj = new JsonObject();
				int i = 1;
				int coreID = rs.getInt(1);
				gm_obj.addProperty(rsmeta.getColumnName(i++), coreID);
				gm_obj.addProperty(rsmeta.getColumnName(i), rs.getInt(i++));
				gm_obj.addProperty(rsmeta.getColumnName(i), rs.getString(i++));
				gm_obj.addProperty(rsmeta.getColumnName(i), rs.getInt(i++));
				gm_obj.addProperty(rsmeta.getColumnName(i), rs.getInt(i++));
				
				for( ; i <= rsmeta.getColumnCount(); i++) {
					gm_obj.addProperty(rsmeta.getColumnName(i), rs.getString(i));
				}
				
				subquery = new Distribution();
				gm_obj.add("distribution", subquery.retRes(gc, Integer.toString(coreID)));
				subquery = new Multimedia();
				gm_obj.add("multimedia", subquery.retRes(gc, Integer.toString(coreID)));
				subquery = new Reference();
				gm_obj.add("references", subquery.retRes(gc, Integer.toString(coreID)));
				subquery = new VernacularName();
				gm_obj.add("vernacularname", subquery.retRes(gc, Integer.toString(coreID)));
				
				gbif_master.add(gm_obj);
			}
		} catch (SQLException sqle) {
			sqle.getErrorCode();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
