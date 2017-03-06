package sql.schema.gbif;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import sql.queries.DbConnection;
import sql.schema.SchemableOM;
import sql.schema.Taxonable;

public class Gbif extends Taxonable {
	private SchemableOM subqueryOM;
	
	public Gbif(DbConnection gc, Gson gson, int lim) {
		super(gc, gson, lim);
		gc.addPrepStmt("taxon", "select * from gbif_taxon gt where kingdom like 'plantae' order by gt.taxonID limit ? offset ?;");
		
		gc.addPrepStmt("dist", "select gd.threatStatus, gd.establishmentMeans, gd.lifeStage, gd.source, gd.country, gd.occuranceStatus, gd.countryCode, gd.locationID, gd.locality, gd.locationRemarks "
				+ "from gbif_taxon gt inner join gbif_distribution gd on gt.taxonID=gd.taxonID where gd.taxonID=?;");
		
		gc.addPrepStmt("mult", "select gm.license, gm.rightsHolder, gm.creator, gm.references, gm.contributor, gm.source, gm.identifier, gm.created, gm.title, gm.publisher, gm.description "
				+ "from gbif_taxon gt inner join gbif_multimedia gm on gt.taxonID=gm.taxonID where gm.taxonID=?;");
		
		gc.addPrepStmt("ref", "select gr.bibliographicCitation,gr.reference,gr.source,gr.identifier "
				+ "from gbif_taxon gt inner join gbif_reference gr on gt.taxonID=gr.taxonID where gr.taxonID=?;");
		
		gc.addPrepStmt("vern", "select gv.sex, gv.lifeStage, gv.source, gv.vernacularName, gv.language, gv.country, gv.countryCode "
				+ "from gbif_taxon gt inner join gbif_vernacularname gv on gt.taxonID=gv.taxonID where gv.taxonID=?;");
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
				int taxonID = rs.getInt(1);

				gm_obj.addProperty(rsmeta.getColumnName(i++), taxonID); //taxonID
				gm_obj.addProperty(rsmeta.getColumnName(i), rs.getString(i++)); //datasetID
				gm_obj.addProperty(rsmeta.getColumnName(i), rs.getInt(i++)); //parentNameUsageID
				gm_obj.addProperty(rsmeta.getColumnName(i), rs.getInt(i++)); //acceptedNameUsageID
				gm_obj.addProperty(rsmeta.getColumnName(i), rs.getInt(i++)); //originalNameUsageID
				
				for( ; i <= rsmeta.getColumnCount(); i++) {
					gm_obj.addProperty(rsmeta.getColumnName(i), rs.getString(i));
				}
				
				subqueryOM = new Distribution();
				gm_obj.add("distribution", subqueryOM.retRes(gc, taxonID));
				subqueryOM = new Multimedia();
				gm_obj.add("multimedia", subqueryOM.retRes(gc, taxonID));
				subqueryOM = new Reference();
				gm_obj.add("references", subqueryOM.retRes(gc, taxonID));
				subqueryOM = new VernacularName();
				gm_obj.add("vernacularname", subqueryOM.retRes(gc, taxonID));

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
