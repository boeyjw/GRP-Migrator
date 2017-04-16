package fx.main;

import java.net.URL;
import java.util.ResourceBundle;

import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.Accordion;
import javafx.scene.control.Button;
import javafx.scene.control.CheckBox;
import javafx.scene.control.ComboBox;
import javafx.scene.control.PasswordField;
import javafx.scene.control.SplitPane;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.control.TitledPane;
import javafx.scene.layout.AnchorPane;
import javafx.scene.layout.GridPane;

public class Controller implements Initializable{
	
	private String[] cliargs;
	
	//Main anchor to entire application
    @FXML
    private AnchorPane mainAnchor;
    
    //Splitter for interface
    @FXML
    private SplitPane splitMainAnchor;
    @FXML
    private AnchorPane argsSplit;
    @FXML
    private SplitPane argsInputSplit;
    @FXML
    private AnchorPane reqargsAnchor;
    
    //All required arguments
    @FXML
    private GridPane reqargsGrid;
    @FXML
    private TextField sqlus;
    @FXML
    private TextField sqldb;
    @FXML
    private PasswordField sqlpw;
    @FXML
    private ComboBox<?> parsedt;
    
    //All optional arguments
    @FXML
    private AnchorPane optargsAnchor;
    @FXML
    private Accordion optAccordion;
    @FXML
    private TitledPane procTitled;
    @FXML
    private TextField sqlpr;
    @FXML
    private TextField sqlsn;
    @FXML
    private TextField sqlba;
    @FXML
    private TextField docbr;
    @FXML
    private CheckBox sernull;
    @FXML
    private TitledPane outputTitled;
    @FXML
    private CheckBox dmdb;
    @FXML
    private TextField outputfn;
    @FXML
    private TextField muri;
    @FXML
    private TextField mdb;
    @FXML
    private TextField mcol;
    
    //The CLI text area
    @FXML
    private AnchorPane cliSplit;
    @FXML
    private TextArea cliTxtOut;
    
    //Execution buttons
    @FXML
    private Button exit;
    @FXML
    private Button reset;
    @FXML
    private Button execute;
    
	@Override
	public void initialize(URL location, ResourceBundle resources) {
		// TODO Auto-generated method stub
		
	}
    
	@FXML
    private void execApp() {

    }

    @FXML
    private void quitApp() {
    	System.exit(0);
    }

    @FXML
    private void resetFields() {
    	sqlus.clear();
    	sqlpw.clear();
    	sqldb.clear();
    	sqlpr.clear();
    	sqlsn.clear();
    	sqlba.clear();
    	docbr.clear();
    	sernull.setSelected(false);
    	dmdb.setSelected(false);
    	outputfn.clear();
    	muri.clear();
    	mdb.clear();
    	mcol.clear();
    }
}

