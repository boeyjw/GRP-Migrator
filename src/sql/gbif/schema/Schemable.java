package sql.gbif.schema;

import com.google.gson.JsonArray;

import sql.queries.GbifConnection;

public interface Schemable {
	public JsonArray retRes(GbifConnection gc, String coreID);
}
