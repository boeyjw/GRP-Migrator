package sql.tojson;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
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
import com.mongodb.MongoConfigurationException;
import com.mongodb.MongoSecurityException;
import com.mongodb.MongoSocketOpenException;
import sql.merger.MergeLinker;
import sql.queries.DbConnection;
import sql.queries.MongoConnection;
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
							.desc("Select ONE parse argument:\n1) ncbi\n2) gbif\n3) acc <DEPRECATED, use ./py/accession.py instead> "
									+ "<Cannot use dmdb switch because of memory issues>\n4) merge")
							.build());
		opt.addOption(Option.builder("ba")
							.longOpt("batchsize")
							.hasArg()
							.argName("NUMROWS")
							.desc("Batch size to process per instance. High batch size consumes more memory. "
									+ "Low batch size reduces performance. Defaults to 200000.")
							.build());
		opt.addOption(Option.builder("fn")
							.longOpt("filename")
							.hasArg()
							.argName("FILENAME")
							.desc("Output file name. Defaults to *databasetype*-out.json.")
							.build());
		opt.addOption(Option.builder("sernull")
							.longOpt("serializenull")
							.desc("Output JSON documents with NULL fields. Default no null field JSON.")
							.build());
		opt.addOption(Option.builder("br")
							.longOpt("breakat")
							.hasArg()
							.argName("NUMROWS")
							.desc("Translate only x number of JSON documents. If x <= batchsize, then only x number of documents are produced.")
							.build());
		opt.addOption(Option.builder("dmdb")
							.longOpt("directmongodb")
							.desc("Does mongoimport directly. If selected, defaults to localhost with default port 27017. Default file output.")
							.build());
		opt.addOption(Option.builder("muri")
							.longOpt("mongodburi")
							.hasArg()
							.argName("MONGODB URI")
							.desc("If dmdb selected and import to server outside localhost. If using cloud database, just paste the host. "
									+ "URI string format: mongodb:\\\\host:port")
							.build());
		opt.addOption(Option.builder("mdb")
							.longOpt("mongodbdb")
							.hasArg()
							.argName("MONGODB DATABASE")
							.desc("The direct MongoDB database to import into. Defaults to SQL database name. Only applicable with dmdb switch.")
							.build());
		opt.addOption(Option.builder("mcol")
							.longOpt("mongodbcol")
							.hasArg()
							.argName("MONGODB COLLECTION")
							.desc("The direct MongoDB collection to import into. Defaults to SQL database type. "
									+ "WARNING: DROPS COLLECTION OF THE SAME NAME! Only applicable with dmdb switch.")
							.build());
		opt.addOption(Option.builder("mpw")
							.longOpt("mongodbpassword")
							.hasArg()
							.argName("MONGODB PASSWORD")
							.desc("If using URI, you can enter your password here or on URI. This option is to elevate the issue of returning to type password in URI.")
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
			//return new Merger(gc, gson, lim); //Deprecated completely
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
			formatter.setOptionComparator(new Comparator<Option>() {
				@Override
				public int compare(Option o1, Option o2) {
					return o1.isRequired() ? -1 : 1;
				}
			});
			formatter.setWidth(100);
			cmd = parser.parse(opt, args);
			if(!cmd.hasOption("us") || !cmd.hasOption("db") || !cmd.hasOption("dt") || cmd.hasOption("h")) {
				formatter.printHelp("java -jar runapp.jar", "Transform SQL rows into JSON\n\n", opt, "Source code repository: https://github.com/boeyjw/gncv2Json", true);
				System.exit(1);
			}
		} catch (ParseException pee) {
			System.err.println(pee.getMessage());
			formatter.printHelp("java -jar runapp.jar", "Transform SQL rows into JSON\n\n", opt, "Source code repository: https://github.com/boeyjw/gncv2Json", true);
			System.exit(2);
		} catch (NullPointerException npe) {
			System.err.println(npe.getMessage());
			formatter.printHelp("java -jar runapp.jar", "Transform SQL rows into JSON\n\n", opt, "Source code repository: https://github.com/boeyjw/gncv2Json", true);
			System.exit(3);
		}

		//Init
		DbConnection gc = CLIConfigurations.getConnectionInstance(cmd);
		//Get batch size
		int lim = (!cmd.hasOption("ba") || cmd.getOptionValue("ba").equals("")) ? 200000 : Integer.parseInt(cmd.getOptionValue("ba"));
		
		//Initialise file name
		String dtnaming = cmd.getOptionValue("dt");
		if(dtnaming.matches("\\d+")) {
			if(dtnaming.matches("4"))
				dtnaming = "merge";
			else if(dtnaming.matches("3"))
				dtnaming = "accession";
			else if(dtnaming.matches("2"))
				dtnaming = "gbif";
			else if(dtnaming.matches("1"))
				dtnaming = "ncbi";
		}
		String fn = !cmd.hasOption("fn") || cmd.getOptionValue("fn").equals("") ? dtnaming.concat("-out.json")
						: cmd.getOptionValue("fn").replaceAll("\\s+", "-").concat(".json");
				
		//Get JSON document limit
		int breakat = cmd.hasOption("br") ? Integer.parseInt(cmd.getOptionValue("br")) : Integer.MIN_VALUE;
		Gson gson = null;
		Taxonable cv = null;
		
		//Initialise Gson object parameters
		gson = cmd.hasOption("sernull") ? new GsonBuilder().serializeNulls().create() : new Gson();
		
		gc.open();
		cv = getTaxonableInit(cmd.getOptionValue("dt"), gc, gson, lim, breakat);
		if(cv == null) {
			formatter.printHelp("java -jar runapp.jar", "Transform SQL rows into JSON\n\n", opt, "Source code repository: https://github.com/boeyjw/gncv2Json", true);
			System.exit(4);
		}

		//Working set
		try {
			int offset = 0;
			JsonWriter arrWriter = null;
			MongoConnection mongodb = null;
			if(cmd.hasOption("dmdb")) {
				try {
					mongodb = cmd.hasOption("muri") ? new MongoConnection(cmd.getOptionValue("muri"), 
							cmd.hasOption("mdb") ? cmd.getOptionValue("mdb") : cmd.getOptionValue("db"), 
									cmd.hasOption("mcol") ? cmd.getOptionValue("mcol") : dtnaming, cmd.hasOption("mpw") ? cmd.getOptionValue("mpw") : "")
							: new MongoConnection(cmd.hasOption("mdb") ? cmd.getOptionValue("mdb") : cmd.getOptionValue("db"), 
									cmd.hasOption("mcol") ? cmd.getOptionValue("mcol") : dtnaming);
				} catch(MongoConfigurationException mce) {
					System.err.println(mce.getMessage());
					System.exit(5);
				} catch(MongoSecurityException mse) {
					System.err.println("Incorrect username or password in URI!");
					System.err.println(mse.getMessage());
					System.exit(5);
				}
				cv.setMongoCollection(mongodb.getMcol());
			}
			else {
				arrWriter = new JsonWriter(new FileWriter(fn));
				cv.setJsonWriter(arrWriter);

				arrWriter.beginArray();
			}
			//Loops until there are no more rows in db
			while(true) {
				if(!cv.taxonToJson(gc, offset, cmd.hasOption("dmdb"))) {
					break;
				}
				offset += lim;
			}
			if(cmd.hasOption("dmdb")) {
				System.out.println();
				mongodb.closeconn();
			}
			else {
				arrWriter.endArray();
				arrWriter.close();
			}
			System.out.println("Successfully processed ".concat(cmd.hasOption("br") ? cmd.getOptionValue("br") : "all") + " documents.");
		} catch (IOException ioe){
			System.err.println(ioe.getMessage());
		} catch (MongoSocketOpenException msoe) {
			System.err.println(msoe.getMessage());
		} catch (SQLException sqle) {
			System.err.println(sqle.getMessage());
		} finally {
			gc.close();
		}
	}
}
