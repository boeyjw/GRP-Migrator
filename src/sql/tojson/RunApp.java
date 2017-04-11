package sql.tojson;

import java.io.FileWriter;
import java.io.IOException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonWriter;

import sql.merger.MergeLinker;
import sql.queries.DbConnection;
import sql.schema.Taxonable;
import sql.schema.gbif.Gbif;
import sql.schema.ncbi.Accession;
import sql.schema.ncbi.Ncbi;

/**
 * The main CLI strategy class to choose output.
 *
 */
public class RunApp {
	
	private static void optionalConfigurations(Options opt) {
		opt.addOption("ba", "batchsize", true, "Batch size to process per instance. High batch size consumes more memory. Low batch size reduces performance. Defaults to 200000.");
		opt.addOption("fn", "filename", true, "Output file name. Defaults to *databasetype*-out.json.");
		opt.addOption("dt", "databasetype", true, "Transform SQL to JSON for dataset:\n\t1) ncbi\n\t2) gbif\n\t3) acc\n\t4) semimerge\n\t5) merge");
		opt.addOption("sernull", "serializenull", false, "Switch between output with null or no null fields. Default: Do not serialize null.");
		opt.addOption("pp", "prettyprint", false, "Outputs human-readable JSON format. Bloats file size due to whitespace.");
		
		opt.getOption("ba").setRequired(false);
		opt.getOption("fn").setRequired(false);
		opt.getOption("dt").setRequired(true);
		opt.getOption("sernull").setRequired(false);
		opt.getOption("pp").setRequired(false);
	}
	
	private static Taxonable getTaxonableInit(String optionValue, DbConnection gc, Gson gson, int lim) {
		if(optionValue.equalsIgnoreCase("ncbi") || optionValue.equals("1")) {
			return new Ncbi(gc, gson, lim);
		}
		else if(optionValue.equalsIgnoreCase("gbif") || optionValue.equals("2")) {
			return new Gbif(gc, gson, lim);
		}
		else if(optionValue.equalsIgnoreCase("acc") || optionValue.equals("3")) {
			//Runnable but deprecated output.
			return new Accession(gc, gson, lim);
		}
		/*else if(optionValue.equalsIgnoreCase("semimerge")) {
			return new SemiMerge(gc, gson, lim);
		}*/
		else if(optionValue.equalsIgnoreCase("merge") || optionValue.equals("4")) {
			return new MergeLinker(gc, gson, lim);
			//return new Merger(gc, gson, lim);
		}
		else {
			System.err.println("Invalid switch for -dt");
			System.exit(1);
		}
		
		return null;
	}
	
	private static void showHelp() {
		System.out.println("gvcn2json - Transform SQL rows into JSON");
		System.out.println("Required options: ");
		System.out.println("-us, -username\t\t\tMySQL username to connect to.");
		System.out.println("-pw, -password\t\t\tMySQL password linked to the username");
		System.out.println("-db, -databasename\t\tMySQL database name to connect to");
		System.out.println("-dt, -databasetype\t\tTransform SQL to JSON for dataset:\n\t1) ncbi\n\t2) gbif\n\t3) acc\n\t4) semimerge\n\t5) merge");
		System.out.println("Optional options: ");
		System.out.println("-sn, -servername\t\tThe server to connect to. Defaults to localhost.");
		System.out.println("-pr, -port\t\t\tThe port to connect. Defaults to 3306.");
		System.out.println("-ba, -batchsize\t\t\tBatch size to process per instance. High batch size consumes more memory. Low batch size reduces performance. Defaults to 200000.");
		System.out.println("-fn, -filename\t\t\tOutput file name. Defaults to *databasetype*-out.json.");
		System.out.println("-sernull, -serializenull\tSwitch between output with null or no null fields. Default: Do not serialize null, false.");
		System.out.println("-h, -help\t\t\tShows this help screen.");
	}
	
	public static void main(String[] args) {
		//CLI
		Options opt = new Options();
		CLIConfigurations.serverConfiguration(opt);
		optionalConfigurations(opt);

		CommandLineParser parser = new DefaultParser();
		HelpFormatter formatter = new HelpFormatter();
		CommandLine cmd = null;

		try {
			cmd = parser.parse(opt, args);
			if(!cmd.hasOption("us") || !cmd.hasOption("pw") || 
					!cmd.hasOption("db") || !cmd.hasOption("dt") ||
					cmd.hasOption("h")) {
				showHelp();
				System.exit(1);
			}
		} catch (ParseException pee) {
			System.out.println(pee.getMessage());
			formatter.printHelp("Transform SQL rows into JSON", opt);
			
			System.exit(1);
			return;
		} catch (NullPointerException npe) {
			System.err.println(npe.getMessage());
		}

		//Init
		DbConnection gc = CLIConfigurations.getConnectionInstance(cmd);
		//Get batch size
		int lim = Integer.parseInt((cmd.getOptionValue("ba") == null || cmd.getOptionValue("ba").equals("")) ? "200000" : cmd.getOptionValue("ba"));
		//Initialise file name
		String fn = (cmd.getOptionValue("fn") == null || cmd.getOptionValue("fn").equals("")) ? 
				(cmd.getOptionValue("db") == null) ? cmd.getOptionValue("dt").concat("-out.json") : cmd.getOptionValue("db").concat("-out.json") 
						: cmd.getOptionValue("fn").concat(".json");
		Gson gson = null;
		Taxonable cv = null;
		
		//Initialise Gson object parameters
		if(cmd.hasOption("sernull")) {
			gson = cmd.hasOption("pp") ? new GsonBuilder().serializeNulls().setPrettyPrinting().create() : new GsonBuilder().serializeNulls().create();
		}
		else {
			gson = cmd.hasOption("pp") ? new GsonBuilder().setPrettyPrinting().create() : new Gson();
		}
		
		gc.open();
		cv = getTaxonableInit(cmd.getOptionValue("dt"), gc, gson, lim);

		//Working set
		try {
			int offset = 0;
			JsonWriter arrWriter;
			arrWriter = new JsonWriter(new FileWriter(fn));
			cv.setJsonWriter(arrWriter);

			arrWriter.beginArray();
			//Loops until there are no more rows in db
			while(true) {
				if(!cv.taxonToJson(gc, offset)) {
					break;
				}
				offset += lim;
			}
			arrWriter.endArray();
			arrWriter.close();
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
