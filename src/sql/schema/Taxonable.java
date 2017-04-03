package sql.schema;

import java.sql.ResultSet;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.stream.JsonWriter;

import sql.queries.DbConnection;

/**
 * 
 * The class which handles the conversion from SQL to JSON.
 * All main schema must be here. 
 * Closely coupled with classes implementing {@link sql.schema.SchemableOM} for one-to-many relationships and
 * {@link sql.schema.SchemableOO} for one-to-one and many-to-one relationships.
 *
 */
public abstract class Taxonable implements Jsonable {
	protected JsonWriter arrWriter;
	protected Gson gson;
	protected int limit;

	protected ProgressBar bar;
	protected JsonObject gm_obj;
	protected ResultSet rs;
	
	/**
	 * Convenient constructor to add all PreparedStatement from parent table into
	 * the HashMap of {@link DbConnection#addPrepStmt(String, String)}. 
	 * @param gc The connection for query purposes
	 */
	public Taxonable() { }
	
	/**
	 * Initialises all prepared statement for the specified database.
	 * @param gc The connection for query purposes
	 */
	public Taxonable(DbConnection gc, Gson gson, int lim) {
		bar = new ProgressBar();
		this.gson = gson;
		this.limit = lim;
	}
	
	/**
	 * Adds all PreparedStatements into the HashMap of {@link DbConnection#addPrepStmt(String, String)}.
	 * Convenient method for merging classes to add all required PreparedStatements from parent tables. 
	 * @param gc
	 */
	public abstract void initQuery(DbConnection gc);

	/**
	 * Explicitly sets the stream writer.
	 * @param arrWriter The reference to the stream writer
	 */
	public void setJsonWriter(JsonWriter arrWriter) {
		this.arrWriter = arrWriter;
	}
}
