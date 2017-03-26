package sql.merger;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import sql.queries.DbConnection;
import sql.schema.Taxonable;

public class MergeLinker extends Taxonable {

	public MergeLinker(DbConnection gc, Gson gson, int lim) {
		super(gc, gson, lim);
		// TODO Auto-generated constructor stub
	}

	@Override
	public boolean taxonToJson(DbConnection gc, int offset) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	protected JsonObject objectify(ResultSet rs, ResultSetMetaData rsmeta, boolean isInt, boolean isI, int loopcount) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

}
