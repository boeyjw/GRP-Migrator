package sql.queries;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class MongoConnection {
	private MongoClient mconn;
	private MongoDatabase mdb;
	private MongoCollection<Document> mcol;
	
	public MongoConnection(String mdb, String mcol) {
		mconn = new MongoClient();
		initDatabaseSettings(mdb, mcol);
	}
	
	public MongoConnection(String uri, String mdb, String mcol) {
		mconn = new MongoClient(uri);
		initDatabaseSettings(mdb, mcol);
		
	}
	
	private void initDatabaseSettings(String mdb, String mcol) {
		this.mdb = mconn.getDatabase(mdb);
		this.mcol = this.mdb.getCollection(mcol);
		this.mcol.drop();
		this.mdb.createCollection(mcol);
		this.mcol = this.mdb.getCollection(mcol);
	}
	
	public void closeconn() {
		mconn.close();
	}
	
	public MongoCollection<Document> getMcol() {
		return mcol;
	}
}
