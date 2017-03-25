package sql.schema;

import java.sql.SQLException;

import com.google.gson.JsonArray;

import sql.queries.DbConnection;

/**
 * 
 * Generalises all schema method.
 * Provides polymorphism to reduce memory overhead from using too many objects.
 * This interface should only and must be implemented by classes which represents the schema of the reference relations with one-to-many relationship.
 * The main schema should be in {@link sql.schema.Taxonable}.
 *
 */
public interface SchemableOM {
	/**
	 * The sub-object of the main object. Always represents a one-to-many relationship.
	 * @return The sub-object of the main relation in Json array.
	 * @throws SQLException
	 */
	public JsonArray retRes() throws SQLException;
	
	
	/**
	 * Check if ResultSet has at least one row.
	 * @param gc The connection for query purposes
	 * @param id The foreign key ID associated with the main relation.
	 * @return True if the ResultSet has at least one row. False otherwise.
	 * @throws SQLException
	 */
	public boolean hasRet(DbConnection gc, int id) throws SQLException;
}
