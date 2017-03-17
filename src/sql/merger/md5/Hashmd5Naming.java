package sql.merger.md5;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import sql.queries.DbConnection;
import sql.tojson.CLIConfigurations;

public class Hashmd5Naming {
	private static Statement stmt;
	private static PreparedStatement[] istmt = new PreparedStatement[4];
	private static ResultSet rs;
	
	private static boolean gnnDone = false;
	private static boolean gvnDone = false;
	private static boolean nneDone = false;
	private static boolean nueDone = false;

	private static void init(Connection con, DbConnection gc, boolean delete) throws SQLException {
		if(delete) {
			stmt = con.createStatement();
			stmt.executeUpdate("drop table if exists `gbif_naming`;");
			stmt.executeUpdate("create table `gbif_naming` ("
					+ "taxonID int(10) unsigned not null, "
					+ "snmd5 char(32) not null, "
					+ "cnmd5 char(32) not null, "
					+ "constraint `pk-gnn-taxonID` primary key (taxonID));");

			stmt.executeUpdate("drop table if exists `gbif_vernaming`;");
			stmt.executeUpdate("create table `gbif_vernaming` ("
					+ "taxonID int(10) unsigned not null, "
					+ "vnmd5 char(32) not null);");

			stmt.executeUpdate("drop table if exists `ncbi_naming`;");
			stmt.executeUpdate("create table `ncbi_naming` ("
					+ "tax_id mediumint(11) unsigned not null, "
					+ "nemd5 char(32) not null);");
			
			stmt.executeUpdate("drop table if exists `ncbi_uqnaming`;");
			stmt.executeUpdate("create table `ncbi_uqnaming` ("
					+ "tax_id mediumint(11) unsigned not null, "
					+ "nemd5 char(32) not null, "
					+ "uqmd5 char(32) not null);");

			stmt.close();
		}
		
		gc.addPrepStmt("gnn", "select taxonID, scientificName, canonicalName from gbif_taxon order by taxonID asc limit ? offset ?;");
		gc.addPrepStmt("gvn", "select taxonID, vernacularName from gbif_vernacularname order by taxonID asc limit ? offset ?;");
		gc.addPrepStmt("nne", "select tax_id, name_txt from ncbi_names order by tax_id asc limit ? offset ?;");
		gc.addPrepStmt("nue", "select tax_id, name_txt, unique_name from ncbi_names where unique_name is not null and ("
				+ "name_class = \"scientific name\" or name_class = \"in-part\" or name_class = \"common name\" or "
				+ "name_class = \" genbank common name\" or name_class = \"synonym\" or name_class = \"anamorph\") "
				+ "order by tax_id limit ? offset ?");

		istmt[0] = con.prepareStatement("insert into gbif_naming (taxonID, snmd5, cnmd5) values (?, ?, ?);");
		istmt[1] = con.prepareStatement("insert into gbif_vernaming (taxonID, vnmd5) values (?, ?);");
		istmt[2] = con.prepareStatement("insert into ncbi_naming (tax_id, nemd5) values (?, ?);");
		istmt[3] = con.prepareStatement("insert into ncbi_uqnaming (tax_id, nemd5, uqmd5) values (?, ?, ?);");
	}

	public static void main(String[] args) throws SQLException {
		Options opt =  new Options();
		CLIConfigurations.serverConfiguration(opt);

		CommandLineParser parser = new DefaultParser();
		HelpFormatter formatter = new HelpFormatter();
		CommandLine cmd = null;

		try {
			cmd = parser.parse(opt, args);
		} catch (ParseException pee) {
			System.out.println(pee.getMessage());
			formatter.printHelp("Transform names into md5 hashes", opt);

			System.exit(1);
			return;
		} catch (NullPointerException npe) {
			System.err.println(npe.getMessage());
		}

		//Init
		DbConnection gc = CLIConfigurations.getConnectionInstance(cmd);
		Md5String md = Md5String.getInstance();
		Connection con = gc.open();
		
		try {
			init(con, gc, true);
			System.out.println("Initialized");
			
			int offset = 0;
			int lim = 10000;
			con.setAutoCommit(false);
			
			while(!(gnnDone && gvnDone && nneDone)) {
				int execSum = 0;
				
				if(!gnnDone) {
					execSum += gnnInsert(gc, lim, offset, md).length;
					System.out.print("gnn: " + gnnDone);
					if(gnnDone) lim *= 1.5;
					con.commit();
					rs.close();
				}
				if(!gvnDone) {
					execSum += gvnInsert(gc, lim, offset, md).length;
					System.out.print("\tgvn: " + gvnDone);
					if(gvnDone) lim *= 1.5;
					con.commit();
					rs.close();
				}
				if(!nneDone) {
					execSum += nneInsert(gc, lim, offset, md).length;
					System.out.print("\tnne: " + nneDone);
					if(nneDone) lim *= 1.5;
					con.commit();
					rs.close();
				}
				if(!nueDone) {
					execSum += nueInsert(gc, lim, offset, md).length;
					System.out.print("\tnne: " + nueDone);
					if(nueDone) lim *= 1.5;
					con.commit();
					rs.close();
				}
				
				offset += lim;
				
				System.out.println("\nTotal execution: " + execSum + "\tCursor at: " + offset);
			}
		} catch (SQLException sqle) {
			System.err.println(sqle.getMessage());
			con.rollback();
		} finally {
			gc.close();
		}
	}

	private static int[] gnnInsert(DbConnection gc, int lim, int offset, Md5String md) throws SQLException {
		rs = gc.selStmt("gnn", new int[] {lim, offset});
		if(!rs.isBeforeFirst()) {
			gnnDone = true;
			return new int[] {};
		}
		
		while(rs.next()) {
			istmt[0].setInt(1, rs.getInt(1));
			istmt[0].setString(2, md.md5HexString(rs.getString(2).toLowerCase().trim()));
			istmt[0].setString(3, md.md5HexString(rs.getString(3).toLowerCase().trim()));
			//System.out.println(rs.getString(2));
			istmt[0].addBatch();
		}
		
		return istmt[0].executeBatch();
	}

	private static int[] gvnInsert(DbConnection gc, int lim, int offset, Md5String md) throws SQLException {
		rs = gc.selStmt("gvn", new int[] {lim, offset});
		if(!rs.isBeforeFirst()) {
			gvnDone = true;
			return new int[] {};
		}
		
		while(rs.next()) {
			istmt[1].setInt(1, rs.getInt(1));
			istmt[1].setString(2, md.md5HexString(rs.getString(2).toLowerCase().trim()));
			//System.out.println(rs.getString(2));
			istmt[1].addBatch();
		}
		
		return istmt[1].executeBatch();
	}

	private static int[] nneInsert(DbConnection gc, int lim, int offset, Md5String md) throws SQLException {
		rs = gc.selStmt("nne", new int[] {lim, offset});
		if(!rs.isBeforeFirst()) {
			nneDone = true;
			return new int[] {};
		}
		
		while(rs.next()) {
			istmt[2].setInt(1, rs.getInt(1));
			istmt[2].setString(2, md.md5HexString(rs.getString(2).toLowerCase().trim()));
			//System.out.println(rs.getString(2));
			istmt[2].addBatch();
		}
		
		return istmt[2].executeBatch();
	}
	
	private static int[] nueInsert(DbConnection gc, int lim, int offset, Md5String md) throws SQLException {
		rs = gc.selStmt("nue", new int[] {lim, offset});
		if(!rs.isBeforeFirst()) {
			nueDone = true;
			return new int[] {};
		}
		
		while(rs.next()) {
			istmt[3].setInt(1, rs.getInt(1));
			istmt[3].setString(2, md.md5HexString(rs.getString(2).toLowerCase().trim()));
			String tmpUq = rs.getString(3);
			//System.out.println(tmpUq);
			//System.out.println(tmpUq.substring(tmpUq.indexOf("<") + 1, tmpUq.indexOf(">")));
			istmt[3].setString(3, md.md5HexString(tmpUq.substring(tmpUq.indexOf("<") + 1, tmpUq.indexOf(">")).toLowerCase().trim()));
			//System.out.println(rs.getString(2));
			istmt[3].addBatch();
		}
		
		return istmt[3].executeBatch();
	}
}
