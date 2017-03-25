package sql.schema.ncbi;

import java.io.IOException;
import java.sql.SQLException;

import com.google.gson.Gson;
import sql.queries.DbConnection;
import sql.schema.Taxonable;

public class Accession extends Taxonable {
	private NuclProt npQuery;
	private String[] np_list;

	public Accession(DbConnection gc, Gson gson, int lim) {
		super(gc, gson, lim);
		gc.addPrepStmt("nodes", "select tax_id from ncbi_nodes nn where nn.division_id=4 order by nn.tax_id limit ? offset ?;");

		gc.addPrepStmt("est", "select nne.accession, nne.`accession.version` as version, nne.gi "
				+ "from ncbi_nodes nn inner join ncbi_nucl_est nne on nne.tax_id=nn.tax_id where nne.tax_id=?;");
		gc.addPrepStmt("estck", "select nne.gi "
				+ "from ncbi_nodes nn inner join ncbi_nucl_est nne on nne.tax_id=nn.tax_id where nne.tax_id=? limit 1;");

		gc.addPrepStmt("wgs", "select nne.accession, nne.`accession.version` as version, nne.gi "
				+ "from ncbi_nodes nn inner join ncbi_nucl_wgs nne on nne.tax_id=nn.tax_id where nne.tax_id=?;");
		gc.addPrepStmt("wgsck", "select nne.gi "
				+ "from ncbi_nodes nn inner join ncbi_nucl_wgs nne on nne.tax_id=nn.tax_id where nne.tax_id=? limit 1;");

		gc.addPrepStmt("gss", "select nne.accession, nne.`accession.version` as version, nne.gi "
				+ "from ncbi_nodes nn inner join ncbi_nucl_gss nne on nne.tax_id=nn.tax_id where nne.tax_id=?;");
		gc.addPrepStmt("gssck", "select nne.gi "
				+ "from ncbi_nodes nn inner join ncbi_nucl_gss nne on nne.tax_id=nn.tax_id where nne.tax_id=? limit 1;");

		gc.addPrepStmt("gb", "select nne.accession, nne.`accession.version` as version, nne.gi "
				+ "from ncbi_nodes nn inner join ncbi_nucl_gb nne on nne.tax_id=nn.tax_id where nne.tax_id=?;");
		gc.addPrepStmt("gbck", "select nne.gi "
				+ "from ncbi_nodes nn inner join ncbi_nucl_gb nne on nne.tax_id=nn.tax_id where nne.tax_id=? limit 1;");

		gc.addPrepStmt("prot", "select nne.accession, nne.`accession.version` as version, nne.gi "
				+ "from ncbi_nodes nn inner join ncbi_prot nne on nne.tax_id=nn.tax_id where nne.tax_id=?;");
		gc.addPrepStmt("protck", "select nne.gi "
				+ "from ncbi_nodes nn inner join ncbi_prot nne on nne.tax_id=nn.tax_id where nne.tax_id=? limit 1;");

		np_list = new String[5];
		np_list[0] = "est";
		np_list[1] = "wgs";
		np_list[2] = "gss";
		np_list[3] = "gb";
		np_list[4] = "prot";
	}

	@Override
	public boolean taxonToJson(DbConnection gc, int offset) throws SQLException {
		try  {
			npQuery = new NuclProt();
			rs = gc.selStmt("nodes", new int[] {lim, offset});
			if(!rs.isBeforeFirst()) {
				return false;
			}

			bar.update(0, lim, Integer.MIN_VALUE);
			while(rs.next()) {
				int tax_id = rs.getInt(1);
				boolean[] selectiveExist = new boolean[np_list.length];
				boolean noneExist = true;

				for(int i = 0; i < np_list.length; i++) {
					if(npQuery.hasRes(gc, tax_id, np_list[i].concat("ck"))) {
						selectiveExist[i] = true;
						noneExist = false;
					}
					else {
						selectiveExist[i] = false;
					}
				}

				if(!noneExist) {
					arrWriter.beginObject();

					arrWriter.name("taxId");
					arrWriter.value(tax_id);

					for(int i = 0; i < np_list.length; i++) {
						if(selectiveExist[i]) {
							NuclProt.querySet = np_list[i];
							arrWriter.name(np_list[i]);
							arrWriter.beginArray();
							npQuery.retRes(gc, new int[] {tax_id}, arrWriter);
							arrWriter.endArray();
						}
					}

					arrWriter.endObject();
				}
				bar.update(rs.getRow(), lim, offset + rs.getRow() + 1);
			}
			rs.close();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		
		return true;
	}

}
