package fx.main;

import java.util.List;

import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.stage.Stage;

public class Main extends Application {
	
	public static List<String> param;
	
	@Override
	public void start(Stage primaryStage) throws Exception {
		//CLI input
		param = getParameters().getUnnamed();
		
		Parent root = FXMLLoader.load(getClass().getResource("gncv2Json_view.fxml"));
		Scene scene = new Scene(root);
		scene.getStylesheets().add(getClass().getResource("gncv2Json_style.css").toExternalForm());
		primaryStage.setScene(scene);
		primaryStage.setTitle("gncv2Json"); 					
		primaryStage.show();
		primaryStage.setResizable(true);
		
	}
	
	public static void main(String[] args) {
		Application.launch(args);
	}

}
