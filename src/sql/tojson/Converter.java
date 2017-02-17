package sql.tojson;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import sql.gbif.schema.Distribution;
import sql.gbif.schema.Multimedia;
import sql.gbif.schema.Reference;
import sql.gbif.schema.Schemable;
import sql.gbif.schema.VernacularName;
import sql.queries.DbConnection;

public class Converter {
	private Schemable subquery;

	public Converter() {}

	public JsonArray makeTaxon(DbConnection gc, String lim, long offset) {
		try {
			JsonObject gm_obj;
			ResultSet rs;
			
			rs = gc.select("select * from gbif_taxon gt order by gt.coreID limit " + lim + " offset " + offset + ";");
			ResultSetMetaData rsmeta = rs.getMetaData();
			JsonArray tmp = new JsonArray();

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

				tmp.add(gm_obj);
			}
			rs.close();
			System.out.println("offset: " + offset);

			return tmp;
		} catch (SQLException sqle) {
			sqle.getErrorCode();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return null;
	}
}
