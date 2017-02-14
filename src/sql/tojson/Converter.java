package sql.tojson;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import sql.queries.GbifConnection;

public class Converter {
	private GbifConnection gc;
	
	private JsonObject gbif_master;
	private Gson gson;
	
	public Converter() {
		gc = new GbifConnection();
		gc.open();
		
		gson = new GsonBuilder().setPrettyPrinting().serializeNulls().create();
		gbif_master = new JsonObject();
	}
	
	public JsonArray arrDoc(ResultSet rs) {
		JsonArray arrj = new JsonArray();
		JsonObject arrdoc = new JsonObject();
		
		try {
			ResultSetMetaData rsmeta = rs.getMetaData();
			
			while(rs.next()) {
				for(int i = 1; i < rsmeta.getColumnCount(); i++) {
					
				}
			}
		} catch (SQLException sqle) {
			sqle.getErrorCode();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
		return arrj;
	}
	
	public static void main(String[] args) {
		
	}
}
