package sql.tojson;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonWriter;

import sql.queries.DbConnection;

public class RunApp {

	public static void main(String[] args) {
		//CLI
		Options opt = new Options();
		opt.addOption("db", "databasename", true, "MySQL database name to connect to");
		opt.addOption("sn", "servername", true, "The server to connect to. If this option is left blank, defaults to localhost");
		opt.addOption("us", "username", true, "MySQL username to connect to.");
		opt.addOption("pw", "password", true, "MySQL password linked tot he username");
		opt.addOption("ba", "batchsize", true, "Batch size");
		opt.addOption("fn", "filename", true, "Output file name");
		opt.addOption("dt", "databasetype", true, "gbif for GBIF db and ncbi for NCBI db");

		opt.getOption("db").setRequired(true);
		opt.getOption("sn").setRequired(false);
		opt.getOption("us").setRequired(true);
		opt.getOption("pw").setRequired(true);
		opt.getOption("ba").setRequired(false);
		opt.getOption("fn").setRequired(false);
		opt.getOption("dt").setRequired(true);

		CommandLineParser parser = new DefaultParser();
		HelpFormatter formatter = new HelpFormatter();
		CommandLine cmd = null;

		try {
			cmd = parser.parse(opt, args);
		} catch (ParseException pee) {
			System.out.println(pee.getMessage());
			formatter.printHelp("Transform GBIF SQL rows into JSON", opt);

			System.exit(1);
			return;
		} catch (NullPointerException npe) {
			System.err.println(npe.getMessage());
		}

		//Init
		DbConnection gc = new DbConnection((cmd.getOptionValue("sn").equals("") || cmd.getOptionValue("sn") == null) ? "localhost" : cmd.getOptionValue("sn"),
				cmd.getOptionValue("db"), cmd.getOptionValue("us"), cmd.getOptionValue("pw"));
		String lim = (cmd.getOptionValue("ba") == null || cmd.getOptionValue("ba").equals("")) ? "200000" : cmd.getOptionValue("ba");
		String fn = (cmd.getOptionValue("fn") == null || cmd.getOptionValue("fn").equals("")) ? cmd.getOptionValue("db").concat("-out.json") : cmd.getOptionValue("fn").concat(".json");
		Gson gson = new GsonBuilder().serializeNulls().create();
		Taxonable cv = null;
		boolean reqBatch = false;

		if(cmd.getOptionValue("dt").equalsIgnoreCase("gbif")) {
			cv = new Gbif(gc, gson, Integer.parseInt(lim));
			reqBatch = false;
		}
		else if(cmd.getOptionValue("dt").equalsIgnoreCase("ncbi")) {
			cv = new Ncbi(gc, gson, Integer.parseInt(lim));
			reqBatch = false;
		}
		else if(cmd.getOptionValue("dt").equalsIgnoreCase("acc")) {
			cv = new Accession(gc, gson, Integer.parseInt(lim));
			reqBatch = true;
		}
		else {
			System.err.println("Invalid switch for -dt");
			System.exit(1);
		}

		//Working set
		try {
			int offset = 0;
			JsonWriter arrWriter;
			if(reqBatch) {
				int fcount = 1;
				Path dir = Files.createDirectory(Paths.get("".concat(fn).replace('.', '-')));
				File ff;
				//Loops until there are no more rows in db
				while(true) {
					ff = new File(dir.toFile(), fcount++ + fn);
					arrWriter = new JsonWriter(new FileWriter(ff));
					cv.setJsonWriter(arrWriter);
					arrWriter.beginArray();
					if(!cv.taxonToJson(gc, offset)) {
						arrWriter.endArray();
						arrWriter.close();
						break;
					}
					offset += Integer.parseInt(lim);
					arrWriter.endArray();
					arrWriter.close();
				}
			} else {
				arrWriter = new JsonWriter(new FileWriter(fn));
				cv.setJsonWriter(arrWriter);

				arrWriter.beginArray();
				//Loops until there are no more rows in db
				while(true) {
					if(!cv.taxonToJson(gc, offset)) {
						break;
					}
					offset += Integer.parseInt(lim);
				}
				arrWriter.endArray();
				arrWriter.close();
			}
		} catch (IOException ioe){
			System.err.println(ioe.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			gc.close();
		}
		System.out.println();
	}
}
