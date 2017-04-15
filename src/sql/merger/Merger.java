package sql.merger;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import sql.queries.DbConnection;
import sql.schema.SchemableOM;
import sql.schema.SchemableOO;
import sql.schema.Taxonable;
import sql.schema.gbif.Distribution;
import sql.schema.gbif.Multimedia;
import sql.schema.gbif.Reference;
import sql.schema.gbif.VernacularName;
import sql.schema.ncbi.Citations;
import sql.schema.ncbi.Division;
import sql.schema.ncbi.Gencode;
import sql.schema.ncbi.Names;

/**
 * This class joins between two database, GBIF and NCBI and assumes they are mutually exclusive.
 * @deprecated Uses outdated database queries.
 *
 */
public class Merger extends Taxonable {
	private SchemableOM subqueryOM;
	private SchemableOO subqueryOO;

	private int i; //rsg
	private int j; //rsn

	private JsonArray arr;

	public Merger(DbConnection gc, Gson gson, int lim, int breakat) {
		super(gc, gson, lim, breakat);
		initQuery(gc);
	}

	@Override
	public boolean taxonToJson(DbConnection gc, int offset, boolean toMongoDB) {
		try {
			rs = gc.selStmt("merge", new int[] {limit, offset});
			if(!rs.isBeforeFirst() || stopPoint(br)) {
				return false;
			}
			//ResultSetMetaData rsmeta = rs.getMetaData();

			bar.update(0, limit, Integer.MIN_VALUE);
			while(rs.next() && !stopPoint(++br)) {
				gm_obj = new JsonObject();
				int taxonID = rs.getInt(1);
				int tax_id = rs.getInt(2);

				ResultSet rsg = gc.selStmt("taxon", new int[] {taxonID});
				ResultSet rsn = gc.selStmt("nodes", new int[] {tax_id});
				ResultSetMetaData rsgmeta = rsg.getMetaData();
				ResultSetMetaData rsnmeta = rsn.getMetaData();

				i = j = 1;

				while(rsg.next() && rsn.next()) {
					gm_obj.addProperty("taxonID", taxonID);
					gm_obj.addProperty("tax_id", tax_id);
					int div_id = rsn.getInt(j++);
					int gen_id = rsn.getInt(j++);
					gm_obj.addProperty(rsgmeta.getColumnLabel(i), rsg.getString(i++)); //datasetID

					gm_obj.addProperty(rsnmeta.getColumnLabel(j), rsn.getInt(j++)); //parent_tax_id
					gm_obj.addProperty(rsnmeta.getColumnLabel(j), rsn.getString(j++)); //rank
					gm_obj.addProperty(rsnmeta.getColumnLabel(j), rsn.getString(j++)); //embl_code
					gm_obj.add("flags", objectify(rsn, rsnmeta, true, false, 6)); //NCBI flagging

					gm_obj.add("usageID", objectify(rsg, rsgmeta, true, true, 3)); //usageIDs
					gm_obj.addProperty(rsgmeta.getColumnLabel(i), rsg.getString(i++)); //scientificName
					gm_obj.addProperty(rsgmeta.getColumnLabel(i), rsg.getString(i++)); //scientificNameAuthorship
					gm_obj.addProperty(rsgmeta.getColumnLabel(i), rsg.getString(i++)); //canonicalName
					gm_obj.add("epithet", objectify(rsg, rsgmeta, false, true, 3)); //Epithet tokens
					for(int z = 0; z < 5; z++) {
						gm_obj.addProperty(rsgmeta.getColumnLabel(i), rsg.getString(i++));
					}
					gm_obj.add("fullTaxonRank", objectify(rsg, rsgmeta, false, true, 6));
					gm_obj.addProperty(rsgmeta.getColumnLabel(i), rsg.getString(i++)); //taxonRemarks
					gm_obj.addProperty(rsnmeta.getColumnLabel(j), rsn.getString(j++)); //comment

					subqueryOM = new Names();
					toIncludeArr("names", subqueryOM.retRes());

					subqueryOO = new Division();
					gm_obj.add("division", subqueryOO.retRes(gc, div_id));

					subqueryOO = new Gencode();
					gm_obj.add("gencode", subqueryOO.retRes(gc, gen_id));

					subqueryOM = new Citations();
					toIncludeArr("citations", subqueryOM.retRes());

					subqueryOM = new Distribution();
					toIncludeArr("distribution", subqueryOM.retRes());

					subqueryOM = new Multimedia();
					toIncludeArr("multimedia", subqueryOM.retRes());

					subqueryOM = new Reference();
					toIncludeArr("references", subqueryOM.retRes());

					subqueryOM = new VernacularName();
					toIncludeArr("vernacularname", subqueryOM.retRes());
				}

				bar.update(rs.getRow() - 1, limit, offset + rs.getRow());
				
				if(toMongoDB)
					addDocument();
				else
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

	private void toIncludeArr(String property, JsonArray subqueryArr) {
		arr = subqueryArr;

		if(arr.size() > 0) {
			gm_obj.add(property, arr);
		}
		else {
			return;
		}
	}

	@Override
	public void initQuery(DbConnection gc) {
		//DB junction tmp table
		gc.addPrepStmt("merge", "select * from gbif_ncbi_junction limit ? offset ?;");

		//ncbi
		gc.addPrepStmt("nodes", "select nn.division_id, nn.genetic_code_id, nn.parent_tax_id as parentTaxId, nn.rank, nn.embl_code as emblCode, "
				+ "nn.inherited_div_flag as inheritedDivFlag, nn.inherited_GC_flag as inheritedGCFlag, nn.mitochondrial_genetic_code_id as mitochondrialGencodeId, "
				+ "nn.inherited_MGC_flag as inheritedMGCFlag, nn.GenBank_hidden_flag as genBankHiddenFlag, nn.hidden_subtree_root_flag as hiddenSubtreeRootFlag, nn.comments "
				+ "from `ncbi`.ncbi_nodes nn where nn.tax_id=?;");

		gc.addPrepStmt("names", "select nnm.name_txt as names, nnm.unique_name as uniqueName, nnm.name_class as nameClass "
				+ "from `ncbi`.ncbi_nodes nn inner join `ncbi`.ncbi_names nnm on nn.tax_id=nnm.tax_id where nnm.tax_id=?;");

		gc.addPrepStmt("div", "select d.division_cde as cde, d.division_name as name, d.comments "
				+ "from `ncbi`.ncbi_division d where d.division_id=?;");

		gc.addPrepStmt("gen", "select g.abbreviation, g.name, g.cde, g.starts "
				+ "from `ncbi`.ncbi_gencode g where g.genetic_code_id=?;");

		gc.addPrepStmt("cit", "select nc.cit_key as citKey, nc.pubmed_id as pubmedId, nc.medline_id as medlineId, nc.url, nc.text "
				+ "from `ncbi`.ncbi_nodes nn left join `ncbi`.ncbi_citations_junction ncj on nn.tax_id=ncj.tax_id left join `ncbi`.ncbi_citations nc on ncj.cit_id=nc.cit_id "
				+ "where nn.tax_id=?;");

		//gbif
		gc.addPrepStmt("taxon", "select gt.datasetID, gt.parentNameUsageID, gt.acceptedNameUsageID, gt.originalNameUsageID, gt.scientificName, gt.scientificNameAuthorship, "
				+ "gt.canonicalName, gt.genericName, gt.specificEpithet, gt.infraspecificEpithet, gt.taxonRank, gt.nameAccordingTo, gt.namePublishedIn, "
				+ "gt.taxonomicStatus, gt.nomenclaturalStatus, gt.kingdom, gt.phylum, gt.class, gt.order, gt.family, gt.genus, gt.taxonRemarks "
				+ "from `gbif`.gbif_taxon gt where gt.taxonID=?;");

		gc.addPrepStmt("dist", "select gd.threatStatus, gd.establishmentMeans, gd.lifeStage, gd.source, gd.country, gd.occuranceStatus, gd.countryCode, gd.locationID, gd.locality, gd.locationRemarks "
				+ "from `gbif`.gbif_taxon gt inner join `gbif`.gbif_distribution gd on gt.taxonID=gd.taxonID where gd.taxonID=?;");

		gc.addPrepStmt("mult", "select gm.license, gm.rightsHolder, gm.creator, gm.references, gm.contributor, gm.source, gm.identifier, gm.created, gm.title, gm.publisher, gm.description "
				+ "from `gbif`.gbif_taxon gt inner join `gbif`.gbif_multimedia gm on gt.taxonID=gm.taxonID where gm.taxonID=?;");

		gc.addPrepStmt("ref", "select gr.bibliographicCitation,gr.references,gr.source,gr.identifier "
				+ "from `gbif`.gbif_taxon gt inner join `gbif`.gbif_reference gr on gt.taxonID=gr.taxonID where gr.taxonID=?;");

		gc.addPrepStmt("vern", "select gv.sex, gv.lifeStage, gv.source, gv.vernacularName, gv.language, gv.country, gv.countryCode "
				+ "from `gbif`.gbif_taxon gt inner join `gbif`.gbif_vernacularname gv on gt.taxonID=gv.taxonID where gv.taxonID=?;");
	}

}
