package sql.schema.ncbi;

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
	//private String[] np_list;

	public Ncbi(DbConnection gc, Gson gson, int lim) {
		super(gc, gson, lim);
		gc.addPrepStmt("nodes", "select * from ncbi_nodes nn where nn.division_id=4 order by nn.tax_id limit ? offset ?;");

		gc.addPrepStmt("names", "select nnm.name_txt, nnm.unique_name, nnm.name_class "
				+ "from ncbi_nodes nn inner join ncbi_names nnm on nn.tax_id=nnm.tax_id where nnm.tax_id=?;");

		gc.addPrepStmt("div", "select d.division_cde as cde, d.division_name as name, d.comments "
				+ "from ncbi_division d where d.division_id=?;");

		gc.addPrepStmt("gen", "select g.abbreviation, g.name, g.cde, g.starts "
				+ "from ncbi_gencode g where g.genetic_code_id=?;");

		gc.addPrepStmt("cit", "select nc.cit_key, nc.pubmed_id, nc.medline_id, nc.url, nc.text "
				+ "from ncbi_nodes nn left join ncbi_citations_junction ncj on nn.tax_id=ncj.tax_id left join ncbi_citations nc on ncj.cit_id=nc.cit_id "
				+ "where nn.tax_id=?;");
	}

	@Override
	public boolean taxonToJson(DbConnection gc, int offset) throws SQLException {

		rs = gc.selStmt("nodes", new int[] {lim, offset});
		if(!rs.isBeforeFirst()) {
			return false;
		}
		ResultSetMetaData rsmeta = rs.getMetaData();

		bar.update(0, lim, Integer.MIN_VALUE);
		while(rs.next()) {
			gm_obj = new JsonObject();
			int i = 1;
			int tax_id = rs.getInt(1);

			gm_obj.addProperty(rsmeta.getColumnName(i++), tax_id); //tax_id
			gm_obj.addProperty(rsmeta.getColumnName(i), rs.getInt(i++)); //parent_tax_id
			gm_obj.addProperty(rsmeta.getColumnName(i), rs.getString(i++)); //rank
			gm_obj.addProperty(rsmeta.getColumnName(i), rs.getString(i++)); //embl_code
			int div_id = rs.getInt(i++); //division_id
			gm_obj.addProperty(rsmeta.getColumnName(i), rs.getInt(i++)); //inherited_div_flag
			int gen_id = rs.getInt(i++); //genetic_code_id

			for( ; i <= rsmeta.getColumnCount() - 1; i++) {
				gm_obj.addProperty(rsmeta.getColumnName(i), rs.getInt(i));
			}

			gm_obj.addProperty(rsmeta.getColumnName(i), rs.getString(i));

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

			bar.update(rs.getRow(), lim, offset + rs.getRow() + 1);
			gson.toJson(gm_obj, arrWriter);
		}
		rs.close();
		//System.out.println("offset: " + offset);

		return true;
	}

}
