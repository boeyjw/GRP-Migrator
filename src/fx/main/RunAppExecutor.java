package fx.main;

import java.io.IOException;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;

import javafx.concurrent.Service;
import javafx.concurrent.Task;

/**
 * Runs the CLI application in another process using a worker thread.
 * Overcomes the GUI memory and CPU utilisation overhead.
 *
 */
public class RunAppExecutor extends Service<Integer> {
	private String[] cliargs;
	
	public void setCLIargs(String[] cliargs) {
		this.cliargs = cliargs;
	}
	
	@Override
	protected Task<Integer> createTask() {
		return new Task<Integer>() {
			@Override
			protected Integer call() throws ExecuteException, IOException {
				CommandLine cmdline = new CommandLine("java");
				cmdline.addArguments(new String[] {"-jar", "\"" + System.getProperty("user.dir") + "/gncv2json.jar" + "\""});
				cmdline.addArguments(cliargs);
				DefaultExecutor exec = new DefaultExecutor();
				exec.setExitValue(0);
				return exec.execute(cmdline);
			}
		};
	}

}
