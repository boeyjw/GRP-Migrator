package sql.schema;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.google.gson.JsonObject;

import sql.queries.DbConnection;

/**
 * Implemented by classes which translates SQL to JSON format.
 *
 */
public interface Jsonable {
	/**
	 * Transform ResultSet from queries into JSON formatted output.
	 * The JSON output is based on the schema which is programmed into the subclasses of this class.
	 * @param gc The database connection for query purposes
	 * @param offset The cursor in which the query limit is currently at
	 * @param toMongoDB Directly insert data into MongoDB if true.
	 * @return True if there may be more rows to be transformed. False if it has reached end of the relation.
	 */
	public abstract boolean taxonToJson(DbConnection gc, int offset, boolean toMongoDB) throws SQLException;
	
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
	public abstract JsonObject objectify(ResultSet rs, ResultSetMetaData rsmeta, boolean isInt, boolean isI, int loopcount) throws SQLException;
}
