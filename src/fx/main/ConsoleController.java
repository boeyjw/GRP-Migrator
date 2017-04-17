package fx.main;

import java.io.PrintStream;
import java.net.URL;
import java.util.ResourceBundle;

import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;
import javafx.concurrent.Service;
import javafx.concurrent.Task;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.TextArea;
import javafx.scene.layout.AnchorPane;

public class ConsoleController extends Service<String> implements Initializable{

    @FXML
    private AnchorPane anchorMain;
    @FXML
    private TextArea cliTxtOut;
    
    private Console console;
    private PrintStream ps;
    
	@Override
	public void initialize(URL location, ResourceBundle resources) {
		cliTxtOut.textProperty().addListener(new ChangeListener<Object>() {
			@Override
			public void changed(ObservableValue<? extends Object> observable, Object oldValue, Object newValue) {
				cliTxtOut.setScrollTop(Double.MAX_VALUE);
			}
		});
		console = new Console(cliTxtOut);
		ps = new PrintStream(console, true);
		System.setErr(ps);
		System.setOut(ps);
	}

	@Override
	protected Task<String> createTask() {
		// TODO Auto-generated method stub
		return null;
	}
}
