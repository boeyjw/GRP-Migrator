package sql.schema;

import java.sql.ResultSet;
import java.util.Map;
import java.util.Set;

import org.bson.Document;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.stream.JsonWriter;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoCollection;

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
	private MongoCollection<Document> mcol;
	
	protected JsonWriter arrWriter;
	protected Gson gson;
	protected int limit;
	protected int breakat;
	protected int br; //Incrementor to track row count of an instant

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
	 * @param breakat TODO
	 */
	public Taxonable(DbConnection gc, Gson gson, int lim, int breakat) {
		bar = new ProgressBar();
		this.gson = gson;
		this.limit = lim;
		this.breakat = breakat;
		br = 0;
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
	
	/**
	 * Explicitly sets the MongoDB collection to insert into.
	 * @param mcol MongoDB collection
	 */
	public void setMongoCollection(MongoCollection<Document> mcol) {
		this.mcol = mcol;
	}
	
	/**
	 * Break at provided number of rows if user uses this option. Has greater priority than {@link #limit}.
	 * @param br Current row count. Incremented per row basis.
	 * @return True if current row count is the breaking point. False otherwise.
	 */
	protected boolean stopPoint(int br) {
		if(this.breakat == Integer.MIN_VALUE) {
			return false;
		}
		return br > this.breakat ? true : false;
	}
	
	/**
	 * Adds a JSON formatted result into MongoDB collection
	 */
	protected void addDocument() {
		try {
			mcol.insertOne(Document.parse(gson.toJson(gm_obj)));
		} catch(MongoWriteException mwe) {
			System.out.println(mwe.getMessage());
		}
	}
	
	/**
	 * Checks if any property of the JSON object is all JSON null
	 * @param obj JSON object to check
	 * @return True if all property of JSON object is null. False otherwise.
	 */
	protected boolean isEmptyObject(JsonObject obj) {
		Set<Map.Entry<String, JsonElement>> setty = obj.entrySet();
		
		for(Map.Entry<String, JsonElement> kv : setty) {
			if(!kv.getValue().isJsonNull())
				return false;
		}
		
		return true;
	}
}
