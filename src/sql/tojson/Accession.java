package sql.tojson;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import sql.queries.DbConnection;
import sql.schema.SchemableOM;
import sql.schema.ncbi.NuclProt;

public class Accession extends Taxonable {
	private SchemableOM subqueryOM;
	private String[] np_list;
	
	public Accession(DbConnection gc, Gson gson, int lim) {
		super(gc, gson, lim);
		gc.addPrepStmt("nodes", "select tax_id from ncbi_nodes nn where nn.division_id=4 order by nn.tax_id limit ? offset ?;");
		
		gc.addPrepStmt("est", "select nne.accession, nne.`accession.version`, nne.gi "
				+ "from ncbi_nodes nn inner join ncbi_nucl_est nne on nne.tax_id=nn.tax_id where nne.tax_id=?;");
		
		gc.addPrepStmt("wgs", "select nne.accession, nne.`accession.version`, nne.gi "
				+ "from ncbi_nodes nn inner join ncbi_nucl_wgs nne on nne.tax_id=nn.tax_id where nne.tax_id=?;");
		
		gc.addPrepStmt("gss", "select nne.accession, nne.`accession.version`, nne.gi "
				+ "from ncbi_nodes nn inner join ncbi_nucl_gss nne on nne.tax_id=nn.tax_id where nne.tax_id=?;");
		
		gc.addPrepStmt("gb", "select nne.accession, nne.`accession.version`, nne.gi "
				+ "from ncbi_nodes nn inner join ncbi_nucl_gb nne on nne.tax_id=nn.tax_id where nne.tax_id=?;");
		
		gc.addPrepStmt("prot", "select nne.accession, nne.`accession.version`, nne.gi "
				+ "from ncbi_nodes nn inner join ncbi_prot nne on nne.tax_id=nn.tax_id where nne.tax_id=?;");
		
		np_list = new String[5];
		np_list[0] = "est";
		np_list[1] = "wgs";
		np_list[2] = "gss";
		np_list[3] = "gb";
		np_list[4] = "prot";
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
				if(tax_id == 3702) continue;
				
				gm_obj.addProperty(rsmeta.getColumnName(i++), tax_id); //tax_id
				
				subqueryOM = new NuclProt();
				for(int j = 0; j < np_list.length; j++) {
					NuclProt.querySet = np_list[j];
					gm_obj.add(np_list[j], subqueryOM.retRes(gc, tax_id));
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

}
