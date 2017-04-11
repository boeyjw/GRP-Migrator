package sql.tojson;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import sql.queries.DbConnection;

/**
 * CLI initialisation.
 * Contains the backbone for using CLI options.
 *
 */
public class CLIConfigurations {
	public static void serverConfiguration(Options opt) {
		opt.addOption("db", "databasename", true, "MySQL database name to connect to");
		opt.addOption("sn", "servername", true, "The server to connect to. If this option is left blank, defaults to localhost");
		opt.addOption("us", "username", true, "MySQL username to connect to.");
		opt.addOption("pw", "password", true, "MySQL password linked to the username");
		opt.addOption("pr", "port", true, "The port to connect. Defaults to 3306.");
		opt.addOption("h", "help", false, "Display help.");

		opt.getOption("db").setRequired(true);
		opt.getOption("sn").setRequired(false);
		opt.getOption("us").setRequired(true);
		opt.getOption("pw").setRequired(true);
		opt.getOption("pr").setRequired(false);
		opt.getOption("h").setRequired(false);
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
