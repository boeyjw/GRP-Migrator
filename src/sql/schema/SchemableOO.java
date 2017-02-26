package sql.schema;

import sql.queries.DbConnection;

import com.google.gson.JsonObject;

/**
 * 
 * Generalises all schema method.
 * Provides polymorphism to reduce memory overhead from using too many objects.
 * This interface should only and must be implemented by classes which represents the schema of the reference relations with many-to-one relationship.
 * The main schema should be in {@link sql.tojson.Taxonable}.
 *
 */
public interface SchemableOO {
	/**
	 * The sub-object of the main object. Always represents a many-to-one relationship.
	 * @param gc The connection for query purposes.
	 * @param id The foreign key ID associated with the main relation.
	 * @return The sub-object of the main relation in Json object.
	 */
	public JsonObject retRes(DbConnection gc, int id);
}
