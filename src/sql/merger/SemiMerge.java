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
import sql.schema.gbif.Multimedia;
import sql.schema.gbif.Reference;
import sql.schema.gbif.VernacularName;
import sql.schema.ncbi.Citations;
import sql.schema.ncbi.Division;
import sql.schema.ncbi.Gencode;
import sql.schema.ncbi.Names;

/**
 * Test class to translate joined GBIF and NCBI database into JSON. Extremely inefficient.
 * @deprecated Uses outdated database queries.
 *
 */
public class SemiMerge extends Taxonable {
	private SchemableOO subqueryOO;
	private SchemableOM subqueryOM;

	public SemiMerge(DbConnection gc, Gson gson, int lim) {
		super(gc, gson, lim);
		initQuery(gc);
	}

	@Override
	public boolean taxonToJson(DbConnection gc, int offset) {
		try {
			rs = gc.selStmt("merge", new int[] {lim, offset});
			if(!rs.isBeforeFirst()) {
				return false;
			}
			//ResultSetMetaData rsmeta = rs.getMetaData();

			bar.update(0, lim, Integer.MIN_VALUE);
			while(rs.next()) {
				gm_obj = new JsonObject();
				int taxonID = rs.getInt(1);
				int tax_id = rs.getInt(2);

				ResultSet rsg = gc.selStmt("taxon", new int[] {taxonID});
				ResultSet rsn = gc.selStmt("nodes", new int[] {tax_id});
				ResultSetMetaData rsgmeta = rsg.getMetaData();
				ResultSetMetaData rsnmeta = rsn.getMetaData();
				int i = 1; //rsg
				int j = 1; //rsn
				while(rsg.next() && rsn.next()) {
					gm_obj.addProperty("taxonID", taxonID);
					gm_obj.addProperty("tax_id", tax_id);
					int div_id = rsn.getInt(j++);
					int gen_id = rsn.getInt(j++);
					gm_obj.addProperty(rsgmeta.getColumnLabel(i), rsg.getString(i++)); //datasetID
					gm_obj.addProperty(rsnmeta.getColumnLabel(j), rsn.getInt(j++)); //parent_tax_id
					gm_obj.addProperty(rsnmeta.getColumnLabel(j), rsn.getString(j++)); //rank
					gm_obj.addProperty(rsnmeta.getColumnLabel(j), rsn.getString(j++)); //embl_code
					for( ; j < rsnmeta.getColumnCount(); j++) {
						gm_obj.addProperty(rsnmeta.getColumnLabel(j), rsn.getInt(j));
					}
					gm_obj.addProperty(rsgmeta.getColumnLabel(i), rsg.getInt(i++)); //parentNameUsageID
					gm_obj.addProperty(rsgmeta.getColumnLabel(i), rsg.getInt(i++)); //acceptedNameUsageID
					gm_obj.addProperty(rsgmeta.getColumnLabel(i), rsg.getInt(i++)); //originalNameUsageID
					for( ; i <= rsgmeta.getColumnCount(); i++) {
						gm_obj.addProperty(rsgmeta.getColumnLabel(i), rsg.getString(i));
					}
					gm_obj.addProperty(rsnmeta.getColumnLabel(j), rsn.getString(j++)); //comment

					subqueryOM = new Names();
					gm_obj.add("names", subqueryOM.retRes());
					subqueryOO = new Division();
					gm_obj.add("division", subqueryOO.retRes(gc, div_id));
					subqueryOO = new Gencode();
					gm_obj.add("gencode", subqueryOO.retRes(gc, gen_id));
					subqueryOM = new Citations();
					gm_obj.add("citations", subqueryOM.retRes());
					subqueryOM = new Distribution();
					gm_obj.add("distribution", subqueryOM.retRes());
					subqueryOM = new Multimedia();
					gm_obj.add("multimedia", subqueryOM.retRes());
					subqueryOM = new Reference();
					gm_obj.add("references", subqueryOM.retRes());
					subqueryOM = new VernacularName();
					gm_obj.add("vernacularname", subqueryOM.retRes());
				}

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

	@Override
	public JsonObject objectify(ResultSet rs, ResultSetMetaData rsmeta, boolean isInt, boolean isI, int loopcount) throws SQLException {
		// Stub. Won't be updated.
		return null;
	}

	@Override
	public void initQuery(DbConnection gc) {
		//DB junction tmp table
		gc.addPrepStmt("merge", "select gn.taxonID, nn.tax_id from `gbif`.gbif_taxon gn inner join `ncbi`.ncbi_names nn on gn.scientificName = nn.name_txt where gn.kingdom like 'Plantae' limit ? offset ?;");

		//ncbi
		gc.addPrepStmt("nodes", "select nn.division_id, nn.genetic_code_id, nn.parent_tax_id, nn.rank, nn.embl_code, nn.inherited_div_flag, nn.inherited_GC_flag, nn.mitochondrial_genetic_code_id, nn.inherited_MGC_flag, nn.GenBank_hidden_flag, nn.hidden_subtree_root_flag, nn.comments "
				+ "from `ncbi`.ncbi_nodes nn where nn.tax_id=?;");

		gc.addPrepStmt("names", "select nnm.name_txt, nnm.unique_name, nnm.name_class "
				+ "from `ncbi`.ncbi_nodes nn inner join `ncbi`.ncbi_names nnm on nn.tax_id=nnm.tax_id where nnm.tax_id=?;");

		gc.addPrepStmt("div", "select d.division_cde as cde, d.division_name as name, d.comments "
				+ "from `ncbi`.ncbi_division d where d.division_id=?;");

		gc.addPrepStmt("gen", "select g.abbreviation, g.name, g.cde, g.starts "
				+ "from `ncbi`.ncbi_gencode g where g.genetic_code_id=?;");

		gc.addPrepStmt("cit", "select nc.cit_key, nc.pubmed_id, nc.medline_id, nc.url, nc.text "
				+ "from `ncbi`.ncbi_nodes nn left join `ncbi`.ncbi_citations_junction ncj on nn.tax_id=ncj.tax_id left join `ncbi`.ncbi_citations nc on ncj.cit_id=nc.cit_id "
				+ "where nn.tax_id=?;");

		//gbif
		gc.addPrepStmt("taxon", "select gt.datasetID, gt.parentNameUsageID, gt.acceptedNameUsageID, gt.originalNameUsageID, gt.scientificName, gt.scientificNameAuthorship, gt.canonicalName, gt.genericName, gt.specificEpithet, gt.infraspecificEpithet, gt.taxonRank, gt.nameAccordingTo, gt.namePublishedIn, gt.taxonomicStatus, gt.nomenclaturalStatus, gt.kingdom, gt.phylum, gt.class, gt.order, gt.family, gt.genus, gt.taxonRemarks "
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
