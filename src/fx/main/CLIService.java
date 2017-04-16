package fx.main;

import javafx.concurrent.Service;
import javafx.concurrent.Task;
import sql.tojson.RunApp;

public class CLIService extends Service<String> {
	private String[] params;
	
	@Override
	protected Task<String> createTask() {
		return new Task<String>() {
			@Override
			protected String call() throws Exception {
				RunApp.main(params);
				return "Completed!";
			}
			
		};
	}
	
	public void setParams(String[] params) {
		this.params = params;
	}
}
