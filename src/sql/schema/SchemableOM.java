package sql.schema;

import com.google.gson.JsonArray;

import sql.queries.DbConnection;

/**
 * 
 * Generalises all schema method.
 * Provides polymorphism to reduce memory overhead from using too many objects.
 * This interface should only and must be implemented by classes which represents the schema of the reference relations with one-to-many relationship.
 * The main schema should be in {@link sql.tojson.Taxonable}.
 *
 */
public interface SchemableOM {
	/**
	 * The sub-object of the main object. Always represents a one-to-many relationship.
	 * @param gc The connection for query purposes.
	 * @param id The foreign key ID associated with the main relation.
	 * @return The sub-object of the main relation in Json array.
	 */
	public JsonArray retRes(DbConnection gc, int id);
}
