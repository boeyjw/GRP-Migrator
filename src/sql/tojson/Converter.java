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
import sql.queries.ProgressBar;

public class Converter {
	private Schemable subquery;

	public Converter() {}

	public JsonArray makeTaxon(DbConnection gc, int lim, int offset) {
		try {
			JsonObject gm_obj;
			ResultSet rs;
			ProgressBar bar = new ProgressBar();
			
			assert gc.addPrepStmt("taxon", "select * from gbif_taxon gt order by gt.coreID limit " + lim + " offset ?;");
			assert gc.addPrepStmt("dist", "select gd.source,gd.threatStatus,gd.locality,gd.lifeStage,gd.occuranceStatus,gd.locationID,gd.locationRemarks,gd.establishmentMeans,gd.countryCode,gd.country "
					+ "from gbif_taxon gt inner join gbif_distribution gd on gt.coreID=gd.coreID where gd.coreID=?");
			assert gc.addPrepStmt("mult", "select gm.references,gm.description,gm.title,gm.contributor,gm.source,gm.created,gm.license,gm.identifier,gm.creator,gm.publisher,gm.rightsHolder "
					+ "from gbif_taxon gt inner join gbif_multimedia gm on gt.coreID=gm.coreID where gm.coreID=?");
			assert gc.addPrepStmt("ref", "select gr.bibliographicCitation,gr.references,gr.source,gr.identifier "
					+ "from gbif_taxon gt inner join gbif_reference gr on gt.coreID=gr.coreID where gr.coreID=?");
			assert gc.addPrepStmt("vern", "select gv.vernacularName,gv.source,gv.sex,gv.lifeStage,gv.language,gv.countryCode,gv.country "
					+ "from gbif_taxon gt inner join gbif_vernacularname gv on gt.coreID=gv.coreID where gv.coreID=?;");
			gc.test();
			rs = gc.selStmt("taxon", 1, offset, lim);
			ResultSetMetaData rsmeta = rs.getMetaData();
			JsonArray tmp = new JsonArray();
			
			//bar.update(0, lim);
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
				gm_obj.add("distribution", subquery.retRes(gc, coreID));
				subquery = new Multimedia();
				gm_obj.add("multimedia", subquery.retRes(gc, coreID));
				subquery = new Reference();
				gm_obj.add("references", subquery.retRes(gc, coreID));
				subquery = new VernacularName();
				gm_obj.add("vernacularname", subquery.retRes(gc, coreID));

				tmp.add(gm_obj);
				//bar.update(rs.getRow(), lim);
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
