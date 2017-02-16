package sql.tojson;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import sql.queries.GbifConnection;

public class RunApp {

	/*public static void main(String[] args) {
		GbifConnection gc = new GbifConnection(args[0], args[1]);
		Gson gson = new GsonBuilder().serializeNulls().setPrettyPrinting().create();
		Converter cv = new Converter();

		cv.makeTaxon(gc);
		gc.close();

		try {
			Writer wr = new FileWriter("gbif-test-out.json");
			wr.write(gson.toJson(cv.getgbifMaster()));
			wr.close();
		} catch (IOException ioe) {
			ioe.getMessage();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}*/
	
	public static void main(String[] args) {
		Options opt = new Options();
		opt.addOption("db", "-databasename", true, "MySQL database name to connect to");
		opt.addOption("sn", "-servername", true, "The server to connect to. If this option is left blank, defaults to localhost");
		opt.addOption("us", "-username", true, "MySQL username to connect to.");
		opt.addOption("pw", "-password", true, "MySQL password linked tot he username");
		
		opt.getOption("db").setRequired(true);
		opt.getOption("sn").setRequired(false);
		opt.getOption("us").setRequired(true);
		opt.getOption("pw").setRequired(true);
		
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
		
		System.out.println(cmd.getOptionValue("sn") + cmd.getOptionValue("db") + cmd.getOptionValue("us") + cmd.getOptionValue("pw"));
		
		GbifConnection gc = new GbifConnection(cmd.getOptionValue("sn"), cmd.getOptionValue("db"), cmd.getOptionValue("us"), cmd.getOptionValue("pw"));
		Gson gson = new GsonBuilder().serializeNulls().setPrettyPrinting().create();
		Converter cv = new Converter();

		cv.makeTaxon(gc);
		gc.close();

		try {
			Writer wr = new FileWriter("gbif-test-out.json");
			wr.write(gson.toJson(cv.getgbifMaster()));
			wr.close();
		} catch (IOException ioe) {
			ioe.getMessage();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
