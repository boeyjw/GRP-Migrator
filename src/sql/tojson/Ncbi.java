package sql.tojson;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import sql.queries.DbConnection;
import sql.schema.SchemableOM;
import sql.schema.SchemableOO;
import sql.schema.ncbi.Division;
import sql.schema.ncbi.Gencode;
import sql.schema.ncbi.Names;

public class Ncbi extends Taxonable {
	private SchemableOO subqueryOO;
	private SchemableOM subqueryOM;
	
	public Ncbi(DbConnection gc, Gson gson, int lim) {
		super(gc, gson, lim);
		gc.addPrepStmt("nodes", "select * from ncbi_nodes nn order by nn.tax_id limit ? offset ?;");
		gc.addPrepStmt("names", "select nnm.name_txt, nnm.unique_name, nnm.name_class "
				+ "from ncbi_nodes nn inner join ncbi_names nnm on nn.tax_id=nnm.tax_id where nnm.tax_id=?;");
		gc.addPrepStmt("div", "select d.cde, d.name, d.comments "
				+ "from ncbi_nodes nn inner join ncbi_division d on nn.division_id=d.division_id where d.division_id=?;");
		gc.addPrepStmt("gen", "select g.abbreviation, g.name, g.cde, g.starts "
				+ "from ncbi_nodes nn inner join ncbi_gencode g on nn.genetic_code_id=g.genetic_code_id where g.genetic_code_id=?;");
	}

	@Override
	public boolean taxonToJson(DbConnection gc, int offset) {
		
		try {
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
				gm_obj.add("names", this.subqueryOM.retRes(gc, tax_id));
				subqueryOO = new Division();
				gm_obj.add("division", subqueryOO.retRes(gc, div_id));
				subqueryOO = new Gencode();
				gm_obj.add("gencode", subqueryOO.retRes(gc, gen_id));
				/*this.subquery = new NuclProt();
				gm_obj.add("", this.subquery.retRes(gc, tax_id));*/

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
