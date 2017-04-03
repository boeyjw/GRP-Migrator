package sql.merger;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import sql.queries.DbConnection;
import sql.schema.SchemableOM;
import sql.schema.SchemableOO;
import sql.schema.Taxonable;
import sql.schema.gbif.Distribution;
import sql.schema.gbif.Gbif;
import sql.schema.gbif.Multimedia;
import sql.schema.gbif.Reference;
import sql.schema.gbif.VernacularName;
import sql.schema.ncbi.Citations;
import sql.schema.ncbi.Division;
import sql.schema.ncbi.Gencode;
import sql.schema.ncbi.Names;
import sql.schema.ncbi.Ncbi;

/**
 * The updated merging class that is being utilised producing correct JSON schema.
 *
 */
public class MergeLinker extends Taxonable {
	private SchemableOM subqueryOM;
	private SchemableOO subqueryOO;
	private int i; //Pointer for GBIF
	private int j; //Pointer for NCBI
	
	public MergeLinker(DbConnection gc, Gson gson, int lim) {
		super(gc, gson, lim);
		initQuery(gc);
	}

	@Override
	public boolean taxonToJson(DbConnection gc, int offset) throws SQLException {
		rs = gc.selStmt("gnjunction", new int[] {lim, offset});
		if(!rs.isBeforeFirst()) {
			return false;
		}

		bar.update(0, lim, Integer.MIN_VALUE);
		while(rs.next()) {
			ResultSet grs = gc.selStmt("taxon", new int[] {rs.getInt(1)});
			ResultSetMetaData grsmeta = grs.getMetaData();
			ResultSet nrs = gc.selStmt("nodes", new int[] {rs.getInt(2)});
			ResultSetMetaData nrsmeta = nrs.getMetaData();
			
			gm_obj = new JsonObject();
			i = j = 1;
			while(grs.next() && nrs.next()) {
				int taxonID = grs.getInt(i);
				int tax_id = nrs.getInt(j);
				gm_obj.addProperty("ncbi_" + nrsmeta.getColumnLabel(j++), tax_id); //tax_id
				int div_id = nrs.getInt(j++);
				int gen_id = nrs.getInt(j++);
				gm_obj.addProperty(nrsmeta.getColumnLabel(j), nrs.getInt(j++)); //parent_tax_id
				
				gm_obj.addProperty("gbif_" + grsmeta.getColumnLabel(i++), taxonID); //taxonID
				gm_obj.addProperty(grsmeta.getColumnLabel(i), grs.getString(i++)); //datasetID
				gm_obj.add("usageID", objectify(grs, grsmeta, true, true, 3)); //Usage IDs
				gm_obj.addProperty(grsmeta.getColumnLabel(i), grs.getString(i++)); //scientificName
				gm_obj.addProperty(grsmeta.getColumnLabel(i), grs.getString(i++)); //scientificNameAuthorship
				gm_obj.addProperty(grsmeta.getColumnLabel(i), grs.getString(i++)); //canonicalName
				gm_obj.add("epithet", objectify(grs, grsmeta, false, true, 3)); //Taxon epithets
				gm_obj.add("nameref", objectify(grs, grsmeta, false, true, 2)); //Taxon name reference
				gm_obj.add("status", objectify(grs, grsmeta, false, true, 2)); //Taxon status
				gm_obj.add("taxontree", objectify(grs, grsmeta, false, true, 6)); //Taxon tree rank
				gm_obj.addProperty(nrsmeta.getColumnLabel(j), nrs.getString(j++)); //rank
				gm_obj.addProperty(nrsmeta.getColumnLabel(j), nrs.getString(j++)); //embl_code
				gm_obj.add("flags", objectify(nrs, nrsmeta, true, false, 6)); //All flags
				gm_obj.addProperty(grsmeta.getColumnLabel(i), grs.getString(i++)); //taxonRemarks
				gm_obj.addProperty(nrsmeta.getColumnLabel(j), nrs.getString(j)); //comments
				
				//Naming
				subqueryOM = new VernacularName();
				if(subqueryOM.hasRet(gc, taxonID)) 
					gm_obj.add("vernacularname", subqueryOM.retRes());
				subqueryOM = new Names();
				if(subqueryOM.hasRet(gc, tax_id))
					gm_obj.add("names", subqueryOM.retRes());
				
				//Misc
				subqueryOM = new Distribution();
				if(subqueryOM.hasRet(gc, taxonID)) 
					gm_obj.add("distribution", subqueryOM.retRes());
				subqueryOM = new Multimedia();
				if(subqueryOM.hasRet(gc, taxonID)) 
					gm_obj.add("multimedia", subqueryOM.retRes());
				subqueryOO = new Division();
				gm_obj.add("division", subqueryOO.retRes(gc, div_id));
				subqueryOO = new Gencode();
				gm_obj.add("gencode", subqueryOO.retRes(gc, gen_id));
				
				//Referencing
				subqueryOM = new Reference();
				if(subqueryOM.hasRet(gc, taxonID)) 
					gm_obj.add("references", subqueryOM.retRes());
				subqueryOM = new Citations();
				if(subqueryOM.hasRet(gc, tax_id))
					gm_obj.add("citations", subqueryOM.retRes());

				bar.update(rs.getRow(), lim, offset + rs.getRow() + 1);
				gson.toJson(gm_obj, arrWriter);
			}
			grs.close();
			nrs.close();
		}
		rs.close();

		return true;
	}

	@Override
	public JsonObject objectify(ResultSet rs, ResultSetMetaData rsmeta, boolean isInt, boolean isI, int loopcount) throws SQLException {
		JsonObject obj = new JsonObject();

		if(isInt) {
			for(int z = 0; z < loopcount; z++) {
				obj.addProperty(rsmeta.getColumnLabel((isI) ? i : j), rs.getInt((isI) ? i++ : j++));
			}
		}
		else {
			for(int z = 0; z < loopcount; z++) {
				obj.addProperty(rsmeta.getColumnLabel((isI) ? i : j), rs.getString((isI) ? i++ : j++));
			}
		}

		return obj;
	}

	@Override
	public void initQuery(DbConnection gc) {
		new Gbif().initQuery(gc); //Add all GBIF queries
		new Ncbi().initQuery(gc); //Add all NCBI queries
		
		//Replace GBIF parent table query
		gc.addPrepStmt("taxon", "select taxonID, datasetID, parentNameUsageID, acceptedNameUsageID, originalNameUsageID, "
				+ "scientificName, scientificNameAuthorship, canonicalName, genericName as generic, specificEpithet as `specific`, "
				+ "infraspecificEpithet as infraspecific, taxonRank, nameAccordingTo, namePublishedIn, taxonomicStatus, nomenclaturalStatus, "
				+ "kingdom, phylum, `class`, `order`, family, genus, taxonRemarks "
				+ "from gbif_taxon where taxonID=?;");
		
		//Replace NCBI parent table query
		gc.addPrepStmt("nodes", "select tax_id as taxId, division_id, genetic_code_id, parent_tax_id as parentTaxId, "
				+ "rank, embl_code, inherited_div_flag as inheritedDivFlag, inherited_GC_flag as inheritedGCFlag, "
				+ "inherited_MGC_flag as inheritedMGCFlag, GenBank_hidden_flag as genBankHiddenFlag, hidden_subtree_root_flag as hiddenSubtreeRootFlag, "
				+ "mitochondrial_genetic_code_id as mitochondrialGeneticCodeId, comments "
				+ "from ncbi_nodes nn where tax_id=?;");
		
		//Get the junctions IDs
		gc.addPrepStmt("gnjunction", "select taxonID, tax_id from gbif_ncbi_junction limit ? offset ?;");
	}

}
