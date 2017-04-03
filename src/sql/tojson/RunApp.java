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
		opt.addOption("ba", "batchsize", true, "Batch size");
		opt.addOption("fn", "filename", true, "Output file name");
		opt.addOption("dt", "databasetype", true, "1) ncbi 2) gbif 3) acc 4) semimerge 5) merge");
		opt.addOption("sernull", "serializenull", true, "Switch between output with null or no null fields. Type 'true' to activate. Default: Serialize null.");
		
		opt.getOption("ba").setRequired(false);
		opt.getOption("fn").setRequired(false);
		opt.getOption("dt").setRequired(true);
		opt.getOption("sernull").setRequired(false);
	}
	
	private static Taxonable getTaxonableInit(String optionValue, DbConnection gc, Gson gson, int lim) {
		if(optionValue.equalsIgnoreCase("gbif")) {
			return new Gbif(gc, gson, lim);
		}
		else if(optionValue.equalsIgnoreCase("ncbi")) {
			return new Ncbi(gc, gson, lim);
		}
		else if(optionValue.equalsIgnoreCase("acc")) {
			//Runnable but deprecated output.
			return new Accession(gc, gson, lim);
		}
		/*else if(optionValue.equalsIgnoreCase("semimerge")) {
			return new SemiMerge(gc, gson, lim);
		}*/
		else if(optionValue.equalsIgnoreCase("merge")) {
			return new MergeLinker(gc, gson, lim);
			//return new Merger(gc, gson, lim);
		}
		else {
			System.err.println("Invalid switch for -dt");
			System.exit(1);
		}
		
		return null;
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
		int lim = Integer.parseInt((cmd.getOptionValue("ba") == null || cmd.getOptionValue("ba").equals("")) ? "200000" : cmd.getOptionValue("ba"));
		String fn = (cmd.getOptionValue("fn") == null || cmd.getOptionValue("fn").equals("")) ? 
				(cmd.getOptionValue("db") == null) ? cmd.getOptionValue("dt").concat("-out.json") : cmd.getOptionValue("db").concat("-out.json") 
						: cmd.getOptionValue("fn").concat(".json");
		Gson gson = null;
		Taxonable cv = null;
		
		if(cmd.getOptionValue("sernull") == null) {
			gson = new Gson();
		}
		else {
			gson = new GsonBuilder().serializeNulls().create();
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
