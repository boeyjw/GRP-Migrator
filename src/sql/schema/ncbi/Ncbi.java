package sql.schema.ncbi;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import sql.queries.DbConnection;
import sql.schema.SchemableOM;
import sql.schema.SchemableOO;
import sql.schema.Taxonable;

public class Ncbi extends Taxonable {
	private SchemableOO subqueryOO;
	private SchemableOM subqueryOM;
	private int i;
	
	public Ncbi() { }
	
	public Ncbi(DbConnection gc, Gson gson, int lim, int breakat) {
		super(gc, gson, lim, breakat);
		initQuery(gc);
	}

	@Override
	public boolean taxonToJson(DbConnection gc, int offset) throws SQLException {
		rs = gc.selStmt("nodes", new int[] {limit, offset});
		if(!rs.isBeforeFirst() || stopPoint(br)) {
			return false;
		}
		ResultSetMetaData rsmeta = rs.getMetaData();

		bar.update(0, limit, Integer.MIN_VALUE);
		while(rs.next() && !stopPoint(++br)) {
			gm_obj = new JsonObject();
			i = 1;
			int tax_id = rs.getInt(i);
			gm_obj.addProperty("ncbi_" + rsmeta.getColumnLabel(i++), tax_id); //tax_id
			int div_id = rs.getInt(i++);
			int gen_id = rs.getInt(i++);
						
			gm_obj.addProperty(rsmeta.getColumnLabel(i), rs.getInt(i++)); //parent_tax_id
			gm_obj.addProperty(rsmeta.getColumnLabel(i), rs.getString(i++)); //rank
			gm_obj.addProperty(rsmeta.getColumnLabel(i), rs.getString(i++)); //embl_code
			gm_obj.add("flags", objectify(rs, rsmeta, true, true, 6)); //All flags
			gm_obj.addProperty(rsmeta.getColumnLabel(i), rs.getString(i)); //comments

			subqueryOM = new Names();
			if(subqueryOM.hasRet(gc, tax_id))
				gm_obj.add("names", subqueryOM.retRes());
			subqueryOO = new Division();
			gm_obj.add("division", subqueryOO.retRes(gc, div_id));
			subqueryOO = new Gencode();
			gm_obj.add("gencode", subqueryOO.retRes(gc, gen_id));
			subqueryOM = new Citations();
			if(subqueryOM.hasRet(gc, tax_id))
				gm_obj.add("citations", subqueryOM.retRes());

			bar.update(rs.getRow() - 1, limit, offset + rs.getRow());
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
	
	public void initQuery(DbConnection gc) {
		gc.addPrepStmt("nodes", "select tax_id as taxId, division_id, genetic_code_id, parent_tax_id as parentTaxId, "
				+ "rank, embl_code, inherited_div_flag as inheritedDivFlag, inherited_GC_flag as inheritedGCFlag, "
				+ "inherited_MGC_flag as inheritedMGCFlag, GenBank_hidden_flag as genBankHiddenFlag, hidden_subtree_root_flag as hiddenSubtreeRootFlag, "
				+ "mitochondrial_genetic_code_id as mitochondrialGeneticCodeId, comments "
				+ "from ncbi_nodes nn where nn.division_id=4 order by nn.tax_id limit ? offset ?;");

		gc.addPrepStmt("names", "select nnm.name_txt as name, nnm.unique_name as uniquename, nnm.name_class as nameclass "
				+ "from ncbi_nodes nn inner join ncbi_names nnm on nn.tax_id=nnm.tax_id where nnm.tax_id=?;");

		gc.addPrepStmt("div", "select d.division_cde as cde, d.division_name as name, d.comments "
				+ "from ncbi_division d where d.division_id=?;");

		gc.addPrepStmt("gen", "select g.abbreviation, g.name, g.cde, g.starts "
				+ "from ncbi_gencode g where g.genetic_code_id=?;");

		gc.addPrepStmt("cit", "select nc.cit_key as citkey, nc.pubmed_id as pubmedId, nc.medline_id as medlineId, nc.url, nc.text "
				+ "from ncbi_nodes nn left join ncbi_citations_junction ncj on nn.tax_id=ncj.tax_id left join ncbi_citations nc on ncj.cit_id=nc.cit_id "
				+ "where nn.tax_id=?;");
	}

}
