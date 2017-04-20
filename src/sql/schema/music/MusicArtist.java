package sql.schema.music;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import sql.queries.DbConnection;
import sql.schema.SchemableOM;
import sql.schema.Taxonable;

public class MusicArtist extends Taxonable {
	private SchemableOM subqueryOM;
	private int i;
	
	public MusicArtist() { }
	
	public MusicArtist(DbConnection gc, Gson gson, int lim, int breakat) {
		super(gc, gson, lim, breakat);
		initQuery(gc);
	}
	
	@Override
	public boolean taxonToJson(DbConnection gc, int offset, boolean toMongoDB) throws SQLException {
		rs = gc.selStmt("artist", new int[] {limit, offset});
		if(!rs.isBeforeFirst() || stopPoint(br)) {
			return false;
		}
		ResultSetMetaData rsmeta = rs.getMetaData();

		bar.update(0, limit, Integer.MIN_VALUE);
		while(rs.next() && !stopPoint(++br)) {
			gm_obj = new JsonObject();
			i = 1;
			int artID = rs.getInt(i);
			
			gm_obj.addProperty(rsmeta.getColumnLabel(i), rs.getInt(i++));
			gm_obj.addProperty(rsmeta.getColumnLabel(i), rs.getString(i++));

			subqueryOM = new CD();
			if(subqueryOM.hasRet(gc, artID))
				gm_obj.add("cd", subqueryOM.retRes());

			bar.update(rs.getRow() - 1, limit, offset + rs.getRow());

			if(toMongoDB)
				addDocument();
			else
				gson.toJson(gm_obj, arrWriter);
		}
		rs.close();

		return true;
	}

	@Override
	public JsonObject objectify(ResultSet rs, ResultSetMetaData rsmeta, boolean isInt, boolean isI, int loopcount)
			throws SQLException {
		//Class doesn't have JSON object reference
		return null;
	}

	@Override
	public void initQuery(DbConnection gc) {
		gc.addPrepStmt("artist", "SELECT artID as artistID, artName as artistName FROM artist ORDER BY artID LIMIT ? OFFSET ?;");
		gc.addPrepStmt("cd", "SELECT cdID, cdTitle as title, cdPrice as price, cdGenre as genre, cdNumOfTracks as numberOfTracks "
				+ "FROM artist INNER JOIN cd USING (artID) WHERE artID = ?;");
		gc.addPrepStmt("track", "SELECT trTitle as title, trRuntime as runtime FROM cd INNER JOIN track USING (cdID) WHERE cdID = ?;");
	}

}
