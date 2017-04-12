package sql.tojson;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Comparator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonWriter;

import sql.merger.MergeLinker;
import sql.merger.SemiMerge;
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
		opt.addOption(Option.builder("dt")
							.longOpt("databasetype")
							.hasArg()
							.required()
							.argName("PARSEARG")
							.desc("Select ONE parse argument:\n1) ncbi\n2) gbif\n3) acc <DEPRECATED, use ./py/accession.py instead>\n4) merge")
							.build());
		opt.addOption(Option.builder("ba")
							.longOpt("batchsize")
							.hasArg()
							.argName("NUMROWS")
							.desc("Batch size to process per instance. High batch size consumes more memory. Low batch size reduces performance. Defaults to 200000.")
							.build());
		opt.addOption(Option.builder("fn")
							.longOpt("filename")
							.hasArg()
							.argName("FILENAME")
							.desc("Output file name. Defaults to *databasetype*-out.json.")
							.build());
		opt.addOption(Option.builder("sernull")
							.longOpt("serializenull")
							.desc("Output JSON documents with NULL fields")
							.build());
		opt.addOption(Option.builder("pp")
							.longOpt("prettyprint")
							.desc("Outputs human-readable JSON format. Will increase file size due to whitespace.")
							.build());
		opt.addOption(Option.builder("br")
							.longOpt("breakat")
							.hasArg()
							.argName("NUMROWS")
							.desc("Translate only x number of JSON documents. If x < batchsize, then only x number of documents are produced.")
							.build());
	}
	
	private static Taxonable getTaxonableInit(String optionValue, DbConnection gc, Gson gson, int lim, int breakat) {
		if(optionValue.equalsIgnoreCase("ncbi") || optionValue.equals("1")) {
			return new Ncbi(gc, gson, lim, breakat);
		}
		else if(optionValue.equalsIgnoreCase("gbif") || optionValue.equals("2")) {
			return new Gbif(gc, gson, lim, breakat);
		}
		else if(optionValue.equalsIgnoreCase("acc") || optionValue.equals("3")) {
			//Runnable but deprecated output.
			return new Accession(gc, gson, lim, breakat);
		}
		/*else if(optionValue.equalsIgnoreCase("semimerge")) {
			//Deprecated completely
			return new SemiMerge(gc, gson, lim);
		}*/
		else if(optionValue.equalsIgnoreCase("merge") || optionValue.equals("4")) {
			return new MergeLinker(gc, gson, lim, breakat);
			//return new Merger(gc, gson, lim);
		}
		else {
			System.err.println("Invalid switch for -dt");
			return null;
		}
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
			formatter.setOptionComparator(new Comparator<Option>() {
				@Override
				public int compare(Option o1, Option o2) {
					return o1.isRequired() ? -1 : 1;
				}
			});
			if(!cmd.hasOption("us") || !cmd.hasOption("pw") || 
					!cmd.hasOption("db") || !cmd.hasOption("dt") ||
					cmd.hasOption("h")) {
				formatter.printHelp("java -jar runapp.jar", "Transform SQL rows into JSON\n\n", opt, "Source code repository: https://github.com/boeyjw/gncv2Json", true);
				System.exit(1);
			}
		} catch (ParseException pee) {
			System.err.println(pee.getMessage());
			formatter.printHelp("java -jar runapp.jar", "Transform SQL rows into JSON\n\n", opt, "Source code repository: https://github.com/boeyjw/gncv2Json", true);
			System.exit(1);
		} catch (NullPointerException npe) {
			System.err.println(npe.getMessage());
			formatter.printHelp("java -jar runapp.jar", "Transform SQL rows into JSON\n\n", opt, "Source code repository: https://github.com/boeyjw/gncv2Json", true);
			System.exit(1);
		}

		//Init
		DbConnection gc = CLIConfigurations.getConnectionInstance(cmd);
		//Get batch size
		int lim = (!cmd.hasOption("ba") || cmd.getOptionValue("ba").equals("")) ? 200000 : Integer.parseInt(cmd.getOptionValue("ba"));
		//Initialise file name
		String fn = (!cmd.hasOption("fn") || cmd.getOptionValue("fn").equals("")) ? 
				(!cmd.hasOption("db")) ? cmd.getOptionValue("dt").concat("-out.json") : cmd.getOptionValue("db").concat("-out.json") 
						: cmd.getOptionValue("fn").replaceAll("\\s+", "-").concat(".json");
		//Get JSON document limit
		int breakat = cmd.hasOption("br") ? Integer.parseInt(cmd.getOptionValue("br")) : Integer.MIN_VALUE;
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
		cv = getTaxonableInit(cmd.getOptionValue("dt"), gc, gson, lim, breakat);
		if(cv == null)
			formatter.printHelp("java -jar runapp.jar", "Transform SQL rows into JSON\n\n", opt, "Source code repository: https://github.com/boeyjw/gncv2Json", true);

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
