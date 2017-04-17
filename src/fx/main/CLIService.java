package fx.main;

import javafx.application.Application;
import javafx.concurrent.Service;
import javafx.concurrent.Task;
import sql.tojson.RunApp;

public class CLIService extends Service<Void> {
	private String[] params;
	
	@Override
	protected Task<Void> createTask() {
		return new Task<Void>() {
			@Override
			protected Void call() throws Exception {
				RunApp.main(params);
				return null;
			}
			
		};
	}
	
	public void setParams(String[] params) {
		this.params = params;
	}
}
