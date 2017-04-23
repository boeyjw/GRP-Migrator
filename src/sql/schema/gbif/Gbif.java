package sql.schema.gbif;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import sql.queries.DbConnection;
import sql.schema.SchemableOM;
import sql.schema.Taxonable;

/**
 * GBIF translation class.
 * Composes of every class in {@link sql.schema.gbif}. 
 * {@link sql.schema.gbif.Distribution}, {@link sql.schema.gbif.Multimedia}, 
 * {@link sql.schema.gbif.Reference}, and {@link sql.schema.gbif.VernacularName}.
 *
 */
public class Gbif extends Taxonable {
	private SchemableOM subqueryOM;
	private int i;
	
	public Gbif() { }
	
	public Gbif(DbConnection gc, Gson gson, int lim, int breakat) {
		super(gc, gson, lim, breakat);
		initQuery(gc);
	}

	@Override
	public boolean taxonToJson(DbConnection gc, int offset, boolean toMongoDB) throws SQLException {
		JsonObject suboo = new JsonObject();
		rs = gc.selStmt("taxon", new int[] {limit, offset});
		if(!rs.isBeforeFirst() || stopPoint(br)) {
			return false;
		}
		ResultSetMetaData rsmeta = rs.getMetaData();

		bar.update(0, limit, Integer.MIN_VALUE);
		while(rs.next() && !stopPoint(++br)) {
			gm_obj = new JsonObject();
			i = 1;
			int taxonID = rs.getInt(1);
			
			gm_obj.addProperty("gbif_" + rsmeta.getColumnLabel(i++), taxonID); //taxonID
			gm_obj.addProperty(rsmeta.getColumnLabel(i), rs.getString(i++)); //datasetID
			suboo = objectify(rs, rsmeta, true, true, 3);
			if(!isEmptyObject(suboo))
				gm_obj.add("usageID", suboo); //Usage IDs
			gm_obj.addProperty(rsmeta.getColumnLabel(i), rs.getString(i++)); //scientificName
			gm_obj.addProperty(rsmeta.getColumnLabel(i), rs.getString(i++)); //scientificNameAuthorship
			gm_obj.addProperty(rsmeta.getColumnLabel(i), rs.getString(i++)); //canonicalName
			suboo = objectify(rs, rsmeta, false, true, 3);
			if(!isEmptyObject(suboo))
				gm_obj.add("epithet", suboo); //Taxon epithets
			gm_obj.addProperty(rsmeta.getColumnLabel(i), rs.getString(i++)); //Taxon rank
			gm_obj.addProperty(rsmeta.getColumnLabel(i), rs.getString(i++)); //Taxon status
			suboo = objectify(rs, rsmeta, false, true, 6);
			if(!isEmptyObject(suboo))
				gm_obj.add("taxontree", suboo); //Taxon tree rank
			gm_obj.addProperty(rsmeta.getColumnLabel(i), rs.getString(i++)); //taxonRemarks

			subqueryOM = new Distribution();
			if(subqueryOM.hasRet(gc, taxonID)) 
				gm_obj.add("distribution", subqueryOM.retRes());
			subqueryOM = new Multimedia();
			if(subqueryOM.hasRet(gc, taxonID)) 
				gm_obj.add("multimedia", subqueryOM.retRes());
			subqueryOM = new Reference();
			if(subqueryOM.hasRet(gc, taxonID)) 
				gm_obj.add("references", subqueryOM.retRes());
			subqueryOM = new VernacularName();
			if(subqueryOM.hasRet(gc, taxonID)) 
				gm_obj.add("vernacularname", subqueryOM.retRes());

			bar.update(rs.getRow() - 1, limit, offset + rs.getRow());

			if(toMongoDB)
				addDocument();
			else
				gson.toJson(gm_obj, arrWriter);
		}
		rs.close();

		return true;
	}

	@Override
	public JsonObject objectify(ResultSet rs, ResultSetMetaData rsmeta, boolean isInt, boolean isI, int loopcount) throws SQLException {
		JsonObject obj = new JsonObject();

		if(isInt) {
			for(int z = 0; z < loopcount; z++) {
				obj.addProperty(rsmeta.getColumnLabel(i), rs.getInt(i++));
			}
		}
		else {
			for(int z = 0; z < loopcount; z++) {
				obj.addProperty(rsmeta.getColumnLabel(i), rs.getString(i++));
			}
		}

		return obj;
	}

	@Override
	public void initQuery(DbConnection gc) {
		gc.addPrepStmt("taxon", "select taxonID, datasetID, parentNameUsageID, acceptedNameUsageID, originalNameUsageID, "
				+ "scientificName, scientificNameAuthorship, canonicalName, genericName as generic, specificEpithet as 'specific', "
				+ "infraspecificEpithet as infraspecific, taxonRank, taxonomicStatus, "
				+ "kingdom, phylum, `class`, `order`, family, genus, taxonRemarks "
				+ "from gbif_taxon gt where kingdom = 'Plantae' order by gt.taxonID limit ? offset ?;");

		gc.addPrepStmt("dist", "select gd.threatStatus, gd.establishmentMeans, gd.lifeStage, gd.source, gd.country, gd.occuranceStatus, "
				+ "gd.countryCode, gd.locationID, gd.locality, gd.locationRemarks "
				+ "from gbif_taxon gt inner join gbif_distribution gd on gt.taxonID=gd.taxonID where gd.taxonID=?;");

		gc.addPrepStmt("mult", "select gm.license, gm.rightsHolder, gm.creator, gm.references, gm.contributor, "
				+ "gm.source, gm.identifier, gm.created, gm.title, gm.publisher, gm.description "
				+ "from gbif_taxon gt inner join gbif_multimedia gm on gt.taxonID=gm.taxonID where gm.taxonID=?;");

		gc.addPrepStmt("ref", "select gr.bibliographicCitation,gr.reference,gr.source,gr.identifier "
				+ "from gbif_taxon gt inner join gbif_reference gr on gt.taxonID=gr.taxonID where gr.taxonID=?;");

		gc.addPrepStmt("vern", "select gv.sex, gv.lifeStage, gv.source, gv.vernacularName as name, gv.language, gv.country, gv.countryCode "
				+ "from gbif_taxon gt inner join gbif_vernacularname gv on gt.taxonID=gv.taxonID where gv.taxonID=?;");
	}

}
