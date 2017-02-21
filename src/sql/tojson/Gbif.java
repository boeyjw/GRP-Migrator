package sql.tojson;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import sql.queries.DbConnection;
import sql.schema.SchemableOM;
import sql.schema.gbif.Distribution;
import sql.schema.gbif.Multimedia;
import sql.schema.gbif.Reference;
import sql.schema.gbif.VernacularName;

public class Gbif extends Taxonable {
	private SchemableOM subquery;
	
	public Gbif(DbConnection gc, Gson gson, int lim) {
		super(gc, gson, lim);
		gc.addPrepStmt("taxon", "select * from gbif_taxon gt where phylum like 'plantae' order by gt.coreID limit ? offset ?;");
		gc.addPrepStmt("dist", "select gd.source,gd.threatStatus,gd.locality,gd.lifeStage,gd.occuranceStatus,gd.locationID,gd.locationRemarks,gd.establishmentMeans,gd.countryCode,gd.country "
				+ "from gbif_taxon gt inner join gbif_distribution gd on gt.coreID=gd.coreID where gd.coreID=?;");
		gc.addPrepStmt("mult", "select gm.references,gm.description,gm.title,gm.contributor,gm.source,gm.created,gm.license,gm.identifier,gm.creator,gm.publisher,gm.rightsHolder "
				+ "from gbif_taxon gt inner join gbif_multimedia gm on gt.coreID=gm.coreID where gm.coreID=?;");
		gc.addPrepStmt("ref", "select gr.bibliographicCitation,gr.references,gr.source,gr.identifier "
				+ "from gbif_taxon gt inner join gbif_reference gr on gt.coreID=gr.coreID where gr.coreID=?;");
		gc.addPrepStmt("vern", "select gv.vernacularName,gv.source,gv.sex,gv.lifeStage,gv.language,gv.countryCode,gv.country "
				+ "from gbif_taxon gt inner join gbif_vernacularname gv on gt.coreID=gv.coreID where gv.coreID=?;");
	}

	@Override
	public boolean taxonToJson(DbConnection gc, int offset) {
		try {
			rs = gc.selStmt("taxon", new int[] {lim, offset});
			if(!rs.isBeforeFirst()) {
				return false;
			}
			ResultSetMetaData rsmeta = rs.getMetaData();
			
			bar.update(0, lim, Integer.MIN_VALUE);
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

				bar.update(rs.getRow(), lim, offset + rs.getRow() + 1);
				gson.toJson(gm_obj, arrWriter);
			}
			rs.close();
			//System.out.println("offset: " + offset);
		} catch (SQLException sqle) {
			sqle.getErrorCode();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return true;
	}

}
