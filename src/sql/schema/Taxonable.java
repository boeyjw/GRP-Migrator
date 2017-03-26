package sql.schema;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

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
public abstract class Taxonable {
	protected JsonWriter arrWriter;
	protected Gson gson;
	protected int lim;

	protected ProgressBar bar;
	protected JsonObject gm_obj;
	protected ResultSet rs;

	/**
	 * Initialises all prepared statement for the specified database. GBIF or NCBI only.
	 * @param gc The connection for query purposes
	 */
	public Taxonable(DbConnection gc, Gson gson, int lim) {
		bar = new ProgressBar();
		this.gson = gson;
		this.lim = lim;
	}

	/**
	 * Transform ResultSet from queries into JSON formatted output.
	 * The JSON output is based on the schema which is programmed into the subclasses of this class.
	 * @param gc The database connection for query purposes
	 * @param offset The cursor in which the query limit is currently at
	 * @return True if there may be more rows to be transformed. False if it has reached end of the relation.
	 */
	public abstract boolean taxonToJson(DbConnection gc, int offset) throws SQLException;
	
	/**
	 * Adds multiple fields into a single JsonObject.
	 * Output: <br \>
	 * {@code "master field 1": { "ref field 1": val, <br \> "ref field 2": val, <br \> "ref field 3": val }
	 * @param rs ResultSet for field value
	 * @param rsmeta ResultSetMetadata for field name
	 * @param isInt True if the value is an int. False otherwise.
	 * @param isI Specifically for classes that has 2 counter values. 
	 * True if class has only 1 counter value or its the first counter value to increment. 
	 * False if second counter value is to be incremented.
	 * @param loopcount The number of reference fields to be added.
	 * @return The JsonObject with all fields attached to it.
	 * @throws SQLException
	 */
	protected abstract JsonObject objectify(ResultSet rs, ResultSetMetaData rsmeta, boolean isInt, boolean isI, int loopcount) throws SQLException;

	/**
	 * Explicitly sets the stream writer as it is outside the try-catch block
	 * @param arrWriter The reference to the stream writer
	 */
	public void setJsonWriter(JsonWriter arrWriter) {
		this.arrWriter = arrWriter;
	}
}
