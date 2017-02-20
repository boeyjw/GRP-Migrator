package sql.tojson;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.stream.JsonWriter;

import sql.queries.DbConnection;
import sql.schema.SchemableOM;
import sql.schema.SchemableOO;
import sql.schema.gbif.*;
import sql.schema.ncbi.*;

/**
 * 
 * The class which handles the conversion from SQL to JSON.
 * All main schema must be here. Closely coupled with classes implementing {@link sql.schema.SchemableOM}.
 *
 */
public class Converter {
	private SchemableOM subquery;
	private ProgressBar bar;
	private JsonObject gm_obj;
	private ResultSet rs;

	/**
	 * Initialises all prepared statement for the specified database. GBIF or NCBI only.
	 * @param gc The connection for query purposes
	 * @param dt The database to be translated
	 */
	public Converter(DbConnection gc, String dt) {
		if(dt.equalsIgnoreCase("gbif")) {
			gc.addPrepStmt("taxon", "select * from gbif_taxon gt order by gt.coreID limit ? offset ?;");
			gc.addPrepStmt("dist", "select gd.source,gd.threatStatus,gd.locality,gd.lifeStage,gd.occuranceStatus,gd.locationID,gd.locationRemarks,gd.establishmentMeans,gd.countryCode,gd.country "
					+ "from gbif_taxon gt inner join gbif_distribution gd on gt.coreID=gd.coreID where gd.coreID=?;");
			gc.addPrepStmt("mult", "select gm.references,gm.description,gm.title,gm.contributor,gm.source,gm.created,gm.license,gm.identifier,gm.creator,gm.publisher,gm.rightsHolder "
					+ "from gbif_taxon gt inner join gbif_multimedia gm on gt.coreID=gm.coreID where gm.coreID=?;");
			gc.addPrepStmt("ref", "select gr.bibliographicCitation,gr.references,gr.source,gr.identifier "
					+ "from gbif_taxon gt inner join gbif_reference gr on gt.coreID=gr.coreID where gr.coreID=?;");
			gc.addPrepStmt("vern", "select gv.vernacularName,gv.source,gv.sex,gv.lifeStage,gv.language,gv.countryCode,gv.country "
					+ "from gbif_taxon gt inner join gbif_vernacularname gv on gt.coreID=gv.coreID where gv.coreID=?;");
		}
		else if(dt.equalsIgnoreCase("ncbi")) {
			gc.addPrepStmt("nodes", "select nn.tax_id, nn.parent_tax_id, nn.rank, nn.embl_code, nn.inherited_div_flag, nn.inherited_GC_flag, "
					+ "nn.inherited_MGC_flag, nn.GenBank_hidden_flag, nn.hidden_subtree_root_flag, nn.comments from ncbi_nodes nn order by nn.tax_id limit ? offset ?;");
			gc.addPrepStmt("names", "select nnm.name_txt, nnm.unique_name, nnm.name_class "
					+ "from ncbi_nodes nn inner join ncbi_names nnm on nn.tax_id=nnm.tax_id where nnm.tax_id=?;");
			gc.addPrepStmt("div", "select d.cde, d.name, d.comments "
					+ "from ncbi_nodes nn inner join ncbi_division d on nn.division_id=d.division_id where d.division_id=?;");
			gc.addPrepStmt("gen", "select g.abbreviation, g.name, g.name, g.cde, g.starts "
					+ "from ncbi_nodes nn inner join ncbi_gencode g on nn.genetic_code_id=g.genetic_code_id where g.genetic_code_id=?;");
		}
		
		bar = new ProgressBar();
	}
	
	/**
	 * Switch for GBIF and NCBI databases. Conversion should be called to here to be delegated.
	 * @param gc The connection for query purposes
	 * @param arrWriter The stream FileWriter
	 * @param gson The Gson configuration to structure the Json format
	 * @param lim The select limit. Should not exceed heap space of java.
	 * @param offset Acts as a pointer to determine where the cursor will end after the limit.
	 * @param dt The database to be translated
	 * @return True if there are more rows to be queried. False if the translation is completed.
	 */
	public boolean makeTaxon(DbConnection gc, JsonWriter arrWriter, Gson gson, int lim, int offset, String dt) {
		if(dt.equalsIgnoreCase("gbif")) {
			return makeGbif(gc, arrWriter, gson, lim, offset);
		}
		else if(dt.equalsIgnoreCase("ncbi")) {
			return makeNcbi(gc, arrWriter, gson, lim, offset);
		}
		
		return false;
	}
	
	private boolean makeNcbi(DbConnection gc, JsonWriter arrWriter, Gson gson, int lim, int offset) {
		SchemableOO subquery;
		try {
			rs = gc.selStmt("nodes", new int[] {lim, offset});
			if(!rs.isBeforeFirst()) {
				return false;
			}
			ResultSetMetaData rsmeta = rs.getMetaData();
			
			//bar.update(0, lim, Integer.MIN_VALUE);
			while(rs.next()) {
				gm_obj = new JsonObject();
				int i = 1;
				int tax_id = rs.getInt(1);

				gm_obj.addProperty(rsmeta.getColumnName(i++), tax_id);
				gm_obj.addProperty(rsmeta.getColumnName(i), rs.getInt(i++));
				gm_obj.addProperty(rsmeta.getColumnName(i), rs.getString(i++));
				gm_obj.addProperty(rsmeta.getColumnName(i), rs.getString(i++));

				for( ; i <= rsmeta.getColumnCount() - 1; i++) {
					gm_obj.addProperty(rsmeta.getColumnName(i), rs.getInt(i));
				}
				
				gm_obj.addProperty(rsmeta.getColumnName(i), rs.getString(i));
				
				this.subquery = new Names();
				gm_obj.add("names", this.subquery.retRes(gc, tax_id));
				subquery = new Division();
				gm_obj.add("div", subquery.retRes(gc, tax_id));
				subquery = new Gencode();
				gm_obj.add("gen", subquery.retRes(gc, tax_id));
				/*this.subquery = new NuclProt();
				gm_obj.add("vern", this.subquery.retRes(gc, tax_id));*/

				//bar.update(rs.getRow(), lim, offset + rs.getRow() + 1);
				gson.toJson(gm_obj, arrWriter);
			}
			rs.close();
			System.out.println("offset: " + offset);
		} catch (SQLException sqle) {
			sqle.getErrorCode();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return true;
	}

	private boolean makeGbif(DbConnection gc, JsonWriter arrWriter, Gson gson, int lim, int offset) {
		try {
			rs = gc.selStmt("taxon", new int[] {lim, offset});
			if(!rs.isBeforeFirst()) {
				return false;
			}
			ResultSetMetaData rsmeta = rs.getMetaData();
			
			bar.update(0, lim, Integer.MIN_VALUE);
			while(rs.next()) {
				gm_obj = new JsonObject();
				int i = 1;
				int coreID = rs.getInt(1);

				gm_obj.addProperty(rsmeta.getColumnName(i++), coreID);
				gm_obj.addProperty(rsmeta.getColumnName(i), rs.getInt(i++));
				gm_obj.addProperty(rsmeta.getColumnName(i), rs.getString(i++));
				gm_obj.addProperty(rsmeta.getColumnName(i), rs.getInt(i++));
				gm_obj.addProperty(rsmeta.getColumnName(i), rs.getInt(i++));

				for( ; i <= rsmeta.getColumnCount(); i++) {
					gm_obj.addProperty(rsmeta.getColumnName(i), rs.getString(i));
				}

				subquery = new Distribution();
				gm_obj.add("distribution", subquery.retRes(gc, coreID));
				subquery = new Multimedia();
				gm_obj.add("multimedia", subquery.retRes(gc, coreID));
				subquery = new Reference();
				gm_obj.add("references", subquery.retRes(gc, coreID));
				subquery = new VernacularName();
				gm_obj.add("vernacularname", subquery.retRes(gc, coreID));

				bar.update(rs.getRow(), lim, offset + rs.getRow() + 1);
				gson.toJson(gm_obj, arrWriter);
			}
			rs.close();
			//System.out.println("offset: " + offset);
		} catch (SQLException sqle) {
			sqle.getErrorCode();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return true;
	}
}
