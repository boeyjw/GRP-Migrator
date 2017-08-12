package sql.schema.ncbi;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.google.gson.stream.JsonWriter;

import sql.queries.DbConnection;

/**
 * NCBI nucleotide and protein accession IDs processing.
 * Composed by {@link sql.schema.ncbi.Accession}.
 * @deprecated {@link sql.schema.ncbi.Accession} is deprecated, this class serves no purpose
 */
public class NuclProt {
	public static String querySet;

	public NuclProt() {
		querySet = new String();
	}
	
	/**
	 * Similar to {@link sql.schema.SchemableOM#retRes()}.
	 * However, this method supports JSON write stream without buffer.
	 * @param gc Database connection
	 * @param param Parameters to be passed to server in prepared statement
	 * @param arrWriter Writer for streaming
	 * @throws SQLException
	 * @throws IOException Write error
	 */
	public void retRes(DbConnection gc, int[] param, JsonWriter arrWriter) throws SQLException, IOException {
		ResultSet rs = gc.selStmt(querySet, param);
		ResultSetMetaData rsmeta = rs.getMetaData();

		while(rs.next()) {
			String accession = null;
			arrWriter.beginObject();
			for(int i = 1; i <= rsmeta.getColumnCount(); i++) {
				try {
					arrWriter.name(rsmeta.getColumnLabel(i).substring(0, 3));
				} catch (IndexOutOfBoundsException iob) {
					arrWriter.name(rsmeta.getColumnLabel(i));
				}
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
	
	/**
	 * Similar to {@link sql.schema.SchemableOM#hasRet(DbConnection, int)}.
	 * However, the ResultSet returned will be entirely stored in memory, which is too large.
	 * @param gc Database connection
	 * @param id Parameter to be passed for prepared statement
	 * @param query Query to be sent, only returns either 1 or no rows
	 * @return True if the associated tax_id has accession IDs. False otherwise.
	 * @throws SQLException
	 */
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
