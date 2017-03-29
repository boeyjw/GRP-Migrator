package sql.schema.ncbi;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.google.gson.stream.JsonWriter;

import sql.queries.DbConnection;

public class NuclProt {
	public static String querySet;

	public NuclProt() {
		querySet = new String();
	}

	public void retRes(DbConnection gc, int[] param, JsonWriter arrWriter) throws SQLException, IOException {
		ResultSet rs = gc.selStmt(querySet, param);
		ResultSetMetaData rsmeta = rs.getMetaData();

		while(rs.next()) {
			String accession = null;
			arrWriter.beginObject();
			for(int i = 1; i <= rsmeta.getColumnCount(); i++) {
				arrWriter.name(rsmeta.getColumnLabel(i));
				if(rsmeta.getColumnLabel(i).equals("accession")) {
					accession = rs.getString(i);
				}
				if(rsmeta.getColumnLabel(i).equals("version")) {
					try {
						arrWriter.value(rs.getString(i).substring(rs.getString(i).indexOf('.')));
					} catch (IndexOutOfBoundsException iob) {
						arrWriter.value(rs.getString(i).replace(accession, ""));
					}
				}
				else if(rsmeta.getColumnLabel(i).equals("gi")) {
					try {
						arrWriter.value(Integer.parseInt(rs.getString(i)));
					} catch (NumberFormatException nfe) {
						arrWriter.value(rs.getString(i));
					}
				}
				else {
					arrWriter.value(rs.getString(i));
				}
			}
			arrWriter.endObject();
		}
		rs.close();

		return;
	}

	public boolean hasRes(DbConnection gc, int id, String query) throws SQLException {
		ResultSet rs = gc.selStmt(query, new int[] {id});
		if(rs.next()) {
			if(!rs.getString(1).isEmpty()) {
				return true;
			}
		}
		rs.close();

		return false;
	}
}
