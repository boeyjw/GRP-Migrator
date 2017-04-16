package fx.main;

import java.io.IOException;
import java.io.OutputStream;

import javafx.scene.control.TextArea;

public class Console extends OutputStream {
	
	private TextArea output;
	
	public Console(TextArea cliTxtOut) {
		output = cliTxtOut;
	}
	
	@Override
	public void write(int b) throws IOException {
		output.appendText(String.valueOf((char) b));
	}

}
