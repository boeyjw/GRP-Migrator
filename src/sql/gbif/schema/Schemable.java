package sql.gbif.schema;

import com.google.gson.JsonArray;

import sql.queries.DbConnection;

public interface Schemable {
	public JsonArray retRes(DbConnection gc, String coreID);
}
