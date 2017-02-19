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

import sql.queries.DbConnection;

public class RunApp {
	
	public static void main(String[] args) {
		Options opt = new Options();
		opt.addOption("db", "databasename", true, "MySQL database name to connect to");
		opt.addOption("sn", "servername", true, "The server to connect to. If this option is left blank, defaults to localhost");
		opt.addOption("us", "username", true, "MySQL username to connect to.");
		opt.addOption("pw", "password", true, "MySQL password linked tot he username");
		opt.addOption("ba", "batchsize", true, "Batch size");
		opt.addOption("fn", "filename", true, "Output file name");
		
		opt.getOption("db").setRequired(true);
		opt.getOption("sn").setRequired(false);
		opt.getOption("us").setRequired(true);
		opt.getOption("pw").setRequired(true);
		opt.getOption("ba").setRequired(false);
		opt.getOption("fn").setRequired(false);
		
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
		}
		
		DbConnection gc = new DbConnection((cmd.getOptionValue("sn").equals("") || cmd.getOptionValue("sn") == null) ? "localhost" : cmd.getOptionValue("sn"),
												cmd.getOptionValue("db"), cmd.getOptionValue("us"), cmd.getOptionValue("pw"));
		String lim = (cmd.getOptionValue("ba") == null || cmd.getOptionValue("ba").equals("")) ? "200000" : cmd.getOptionValue("ba");
		String fn = (cmd.getOptionValue("fn") == null || cmd.getOptionValue("fn").equals("")) ? cmd.getOptionValue("db").concat("out.json") : cmd.getOptionValue("fn");
		Gson gson = new GsonBuilder().serializeNulls().create();
		Converter cv = new Converter(gc);

		try {
			int offset = 0;
			JsonWriter arrWriter = new JsonWriter(new FileWriter(fn));
			
			arrWriter.beginArray();
			while(true) {
				if(!cv.makeTaxon(gc, arrWriter, gson, Integer.parseInt(lim), offset)) {
					break;
				}
				offset += Integer.parseInt(lim);
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
