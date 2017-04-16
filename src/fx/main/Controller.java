package fx.main;

import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ResourceBundle;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.Accordion;
import javafx.scene.control.Alert;
import javafx.scene.control.Alert.AlertType;
import javafx.scene.control.Button;
import javafx.scene.control.CheckBox;
import javafx.scene.control.ComboBox;
import javafx.scene.control.PasswordField;
import javafx.scene.control.SplitPane;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.control.TitledPane;
import javafx.scene.control.Tooltip;
import javafx.scene.layout.AnchorPane;
import javafx.scene.layout.GridPane;
import sql.tojson.RunApp;

public class Controller implements Initializable{
	
	private List<String> cliargs;
	
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
    @FXML
    private SplitPane optargsSplit;
    
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
    private ComboBox<String> parsedt;
    
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
    
    //Integer only validation regex
    private Pattern digitPat = Pattern.compile("\\d+");
	private Matcher digitmat = digitPat.matcher("");
    
	@Override
	public void initialize(URL location, ResourceBundle resources) {
		cliTxtOut.setEditable(false);
		if(!Main.param.isEmpty())
			executeRunApp(Main.param.toArray(new String[0]));
		else {
			cliargs = new ArrayList<String>();
			initReqArgs();
			initOptArgsProcessing();
			initOptArgsOutput();
		}
	}
	
	private void initReqArgs() {
		sqlus.setPromptText("root");
		sqlus.setTooltip(new Tooltip("Enter SQL username. Defaults to root."));
		
		sqlpw.setTooltip(new Tooltip("Enter SQL password. Defaults to no password."));
		
		sqldb.setTooltip(new Tooltip("Enter SQL database to access. Must have an entry."));
		sqldb.focusedProperty().addListener((arg0, oldValue, newValue) -> {
			if(!newValue) {
				if(sqldb.getText() == null ||sqldb.getText().trim().isEmpty()) {
					//validation error! Stop them from executing!
				}
				else {
					mdb.setPromptText(sqldb.getText().trim());
				}
			}
		});
		
		parsedt.setItems(FXCollections.observableList(Arrays.asList((new String[]{"ncbi", "gbif", "acc <deprecated>", "merge"}))));
		parsedt.focusedProperty().addListener((arg0, oldValue, newValue) -> {
			outputfn.setPromptText(parsedt.getValue());
			mcol.setPromptText(parsedt.getValue());
		});
	}
	
	private void initOptArgsProcessing() {
		sqlpr.setTooltip(new Tooltip("Enter SQL port. Defaults to 3306."));
		sqlpr.setPromptText("3306");
		sqlpr.focusedProperty().addListener((arg0, oldValue, newValue) -> {
			if(!newValue) {
				digitmat.reset(sqlpr.getText());
				if(!digitmat.matches()) {
					//validation error! Stop them from executing!
				}
			}
		});
		
		sqlsn.setTooltip(new Tooltip("Enter SQL host. Defaults to localhost."));
		sqlsn.setPromptText("localhost");
		
		sqlba.setTooltip(new Tooltip("Enter batch size to process. Large batch size consumes more memory. "
				+ "Small batch size reduces performance. Defaults to 200000"));
		sqlba.setPromptText("200000");
		sqlba.focusedProperty().addListener((arg0, oldValue, newValue) -> {
			if(!newValue) {
				digitmat.reset(sqlba.getText());
				if(!digitmat.matches()) {
					//validation error! Stop them from executing!
				}
			}
		});
		
		docbr.setTooltip(new Tooltip("Enter max rows to process. Defaults to ALL."));
		docbr.focusedProperty().addListener((arg0, oldValue, newValue) -> {
			if(!newValue) {
				digitmat.reset(docbr.getText());
				if(!digitmat.matches()) {
					//validation error! Stop them from executing!
				}
			}
		});
	}
	
	private void initOptArgsOutput() {
		muri.setDisable(true);
		mdb.setDisable(true);
		mcol.setDisable(true);
		dmdb.selectedProperty().addListener(new ChangeListener<Boolean>() {
			@Override
			public void changed(ObservableValue<? extends Boolean> observable, Boolean oldValue, Boolean newValue) {
				if(dmdb.isSelected()) {
					outputfn.setDisable(true);
					muri.setDisable(false);
					mdb.setDisable(false);
					mcol.setDisable(false);
				}
				else {
					outputfn.setDisable(false);
					muri.setDisable(true);
					mdb.setDisable(true);
					mcol.setDisable(true);
				}
			}
		});
		
		outputfn.setTooltip(new Tooltip("Enter output filename. Defaults to *databasetype*.json."));
		
		muri.setTooltip(new Tooltip("Enter or paste URI here. Defaults to mongodb://localhost:27017."));
		muri.setPromptText("mongodb://localhost:27017");
		
		mdb.setTooltip(new Tooltip("Enter MongoDB database to insert into. Defaults to *database name*."));
		
		mcol.setTooltip(new Tooltip("Enter MongoDB collection to insert into. Defaults to *database type*."));
	}
	
	private void executeRunApp(String[] args) {
		if(args.length >= 4)
			RunApp.main(args);
	}
    
	@FXML
    private void execApp() {	
		validateAndAppendReqArgs();
		
		if(sqlpr.getText() != null || !sqlpr.getText().trim().isEmpty()) {
			digitmat.reset(sqlpr.getText());
			if(digitmat.matches())
				addCLIargs("-pr", sqlpr.getText().trim());
		}
		if(sqlsn.getText() != null || !sqlsn.getText().trim().isEmpty())
			addCLIargs("-sn", sqlsn.getText().trim());
		if(sqlba.getText() != null || !sqlba.getText().trim().isEmpty()) {
			digitmat.reset(sqlba.getText());
			if(digitmat.matches())
				addCLIargs("-ba", sqlba.getText().trim());
		}
		if(docbr.getText() != null || !docbr.getText().trim().isEmpty()) {
			digitmat.reset(docbr.getText());
			if(digitmat.matches())
				addCLIargs("-br", docbr.getText().trim());
		}
		if(sernull.isSelected())
			addCLIargs("-sernull", "");
		
		if(dmdb.isSelected()) {
			addCLIargs("-dmdb", "");
			if(muri.getText() != null || !muri.getText().trim().isEmpty())
				addCLIargs("-muri", muri.getText().trim());
			if(mdb.getText() != null || !mdb.getText().trim().isEmpty())
				addCLIargs("-mdb", mdb.getText().trim());
			if(mcol.getText() != null || !mcol.getText().trim().isEmpty())
				addCLIargs("-mcol", mcol.getText().trim());
		}
		else {
			if(outputfn.getText() != null || !outputfn.getText().trim().isEmpty())
				addCLIargs("-fn", outputfn.getText().trim());
		}
    }
	
	private void addCLIargs(String flag, String value) {
		cliargs.add(flag);
		cliargs.add(value);
	}
	
	private void validateAndAppendReqArgs() {
		String error = "";
		
		if(sqldb.getText() == null || sqldb.getText().trim().isEmpty())
			error += "SQL database";
		
		if(error.isEmpty()) {
			addCLIargs("-us", sqlus.getText() == null || sqlus.getText().trim().isEmpty() ? "root" : sqlus.getText().trim());
			addCLIargs("-pw", sqlpw.getText() == null || sqlpw.getText().trim().isEmpty() ? "" : sqlpw.getText().trim());
			addCLIargs("-db", sqldb.getText().trim());
			addCLIargs("-dt", parsedt.getValue());
		}
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
    	parsedt.setValue("Parse?");
    	sernull.setSelected(false);
    	dmdb.setSelected(false);
    	outputfn.clear();
    	muri.clear();
    	mdb.clear();
    	mcol.clear();
    	
    	cliargs.clear();
    	
    	initReqArgs();
    	initOptArgsProcessing();
    	initOptArgsOutput();
    }
}

