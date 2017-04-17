package sql.queries;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoConfigurationException;
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
		Pattern uripat = Pattern.compile("^(mongodb://.+)(:.*@)(.*)$");
		Matcher urimat = uripat.matcher(uri);
		String seturi = urimat.matches() && !mpw.isEmpty() ? urimat.group(1).concat(":" + mpw + "@").concat(urimat.group(3)) : uri;

		uripat = Pattern.compile("^(mongodb://.*)(/.+\\?)(.*)$");
		urimat = uripat.matcher(seturi);
		seturi = urimat.matches() ? urimat.group(1).concat("/?").concat(urimat.group(3)) : seturi;
		
		if(!seturi.matches("^mongodb://.+:.*@.*/?.*$"))
			throw new MongoConfigurationException("URI string invalid!");
		mconn = new MongoClient(new MongoClientURI(seturi));
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
