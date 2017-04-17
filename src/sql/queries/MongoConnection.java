package sql.queries;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoConfigurationException;
import com.mongodb.MongoSecurityException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 * Handles single MongoDB connection for direct insertion.
 *
 */
public class MongoConnection {
	private MongoClient mconn;
	private MongoDatabase mdb;
	private MongoCollection<Document> mcol;
	
	/**
	 * Default MongoDB connection to localhost and port 27017.
	 * @param mdb MongoDB database
	 * @param mcol MongoDB collection
	 */
	public MongoConnection(String mdb, String mcol) {
		mconn = new MongoClient();
		initDatabaseSettings(mdb, mcol);
	}
	
	/**
	 * Connects to a remote MongoDB server via URI.
	 * @param uri MongoDB URI
	 * @param mdb MongoDB database
	 * @param mcol MongoDB collection
	 */
	public MongoConnection(String uri, String mdb, String mcol, String mpw) {
		uri = uri.contains("<PASSWORD>") && !mpw.isEmpty() && !uri.matches(":.@") ? uri.replace("<PASSWORD>", mpw) : uri;
		uri = uri.contains("<DATABASE>") ? uri.replace("<DATABASE>", "")
				: uri.matches("/.?") ? uri.replaceFirst("/.?", "/?") : uri;
		if(!uri.contains("/?") || uri.matches(":.@"))
			throw new MongoConfigurationException("URI string invalid!");
		mconn = new MongoClient(new MongoClientURI(uri));
		initDatabaseSettings(mdb, mcol);
	}
	
	/**
	 * Initialises MongoDB database settings.
	 * Drops collection that exists.
	 * @param mdb MongoDB database
	 * @param mcol MongoDB collection
	 */
	private void initDatabaseSettings(String mdb, String mcol) {
		this.mdb = mconn.getDatabase(mdb);
		this.mcol = this.mdb.getCollection(mcol);
		this.mcol.drop();
		this.mdb.createCollection(mcol);
		this.mcol = this.mdb.getCollection(mcol);
	}
	
	/**
	 * Closes MongoDB connection
	 */
	public void closeconn() {
		mconn.close();
	}
	
	/**
	 * Returns MongoDB collection object for insertion purposes.
	 * @return MongoDB collection object
	 */
	public MongoCollection<Document> getMcol() {
		return mcol;
	}
}
