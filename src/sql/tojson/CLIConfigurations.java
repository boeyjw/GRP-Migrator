package sql.tojson;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import sql.queries.DbConnection;

/**
 * CLI initialisation.
 * Contains the backbone for using CLI options.
 *
 */
public class CLIConfigurations {
	
	public static void serverConfiguration(Options opt) {
		opt.addOption(Option.builder("us")
							.longOpt("username")
							.hasArg()
							.required()
							.argName("USERNAME")
							.desc("SQL server username. Enter \"\" if no password.")
							.build());
		opt.addOption(Option.builder("pw")
							.longOpt("password")
							.hasArg()
							.required()
							.argName("PASSWORD")
							.desc("SQL server password.")
							.build());
		opt.addOption(Option.builder("db")
							.longOpt("databasename")
							.hasArg()
							.required()
							.argName("DATABASE")
							.desc("SQL database to query from.")
							.build());
		opt.addOption(Option.builder("pr")
							.longOpt("port")
							.hasArg()
							.argName("PORT")
							.desc("SQL server port.")
							.build());
		opt.addOption(Option.builder("sn")
							.longOpt("servername")
							.hasArg()
							.argName("HOST")
							.desc("SQL server host. Defaults to localhost.")
							.build());
		opt.addOption(Option.builder("h")
							.longOpt("help")
							.desc("Display this help.")
							.build());
	}
	
	public static DbConnection getConnectionInstance(CommandLine cmd) {
		if(cmd.getOptionValue("db") == null) {
			return new DbConnection(cmd.getOptionValue("sn"), cmd.getOptionValue("us"), cmd.getOptionValue("pw"));
		}
		else if(cmd.getOptionValue("pr") == null) {
			return new DbConnection(cmd.getOptionValue("sn"), cmd.getOptionValue("db"), cmd.getOptionValue("us"), cmd.getOptionValue("pw"));
		}
		else {
			return new DbConnection(cmd.getOptionValue("sn"), Integer.parseInt(cmd.getOptionValue("pr")), cmd.getOptionValue("db"), cmd.getOptionValue("us"), cmd.getOptionValue("pw"));
		}
	}
}
