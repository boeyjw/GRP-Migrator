package sql.schema.ncbi;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.google.gson.JsonObject;

import sql.queries.DbConnection;
import sql.schema.SchemableOO;

/**
 * NCBI division schema
 *
 */
public class Division implements SchemableOO {
	private JsonObject obj;

	public Division() {
		obj = new JsonObject();
	}

	@Override
	public JsonObject retRes(DbConnection gc, int id) throws SQLException {

		ResultSet rs = gc.selStmt("div", new int[] {id});
		ResultSetMetaData rsmeta = rs.getMetaData();

		if(rs.next()) {
			for(int i = 1; i <= rsmeta.getColumnCount(); i++) {
				obj.addProperty(rsmeta.getColumnLabel(i), rs.getString(i));
			}
		}
		rs.close();

		return obj;
	}

}
