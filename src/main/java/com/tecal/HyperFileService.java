package com.tecal;


import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class HyperFileService { 
	private static String JDBC_URL = "jdbc:h2:~/test"; // Remplacez par l'URL de votre serveur HyperFile 
	private static String USER = "admin"; // Remplacez par votre utilisateur 
	private static String PASSWORD = ""; // Remplacez par votre mot de passe 
	private static final Logger logger = LogManager.getLogger(HyperFileService.class);
	private static boolean running = true;
	
    public static void main(String[] args) {
    	// Use the JDBC driver of Microsoft
    	try {
			Class.forName("com.ms.jdbc.odbc.JdbcOdbcDriver");
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			logger.error("Erreur lors du chargement com.ms.jdbc.odbc.JdbcOdbcDriver : " + e.getMessage());
		}
        // Charger la configuration depuis le fichier config.ini
        loadDatabaseConfig("config.ini");
        if (args.length > 0) {
            switch (args[0]) {
                case "start":
                    start();
                    break;
                case "stop":
                    stop();
                    break;
                case "test":
                    execute();
                    break;
                default:
                    System.out.println("Usage: java HyperFileService <start|stop>");
                    break;
            }
        } else {
            System.out.println("Usage: java HyperFileService <start|stop>");
        } 


       
    }

	private static void execute() {
		ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

        Runnable task = () -> {
            try (Connection connection = DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
                 Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery("SELECT * FROM your_table")) {
            	
            	String timestamp = new java.text.SimpleDateFormat("yyyyMMdd_HHmmss").format(new java.util.Date());
            	String outputFileName = "output_" + timestamp + ".csv";

                try (CSVPrinter printer = new CSVPrinter(new FileWriter(outputFileName, true), CSVFormat.DEFAULT.withHeader(resultSet))) {
                    while (resultSet.next()) {
                        int columnCount = resultSet.getMetaData().getColumnCount();
                        Object[] row = new Object[columnCount];
                        for (int i = 0; i < columnCount; i++) {
                            row[i] = resultSet.getObject(i + 1);
                        }
                        printer.printRecord(row);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }

            } catch (SQLException e) {
                e.printStackTrace();
            }
        };

        // Planifie la tâche pour s'exécuter toutes les 5 minutes
        scheduler.scheduleAtFixedRate(task, 0, 5, TimeUnit.MINUTES);
	}

    /**
     * Charge les informations de connexion à la base de données depuis un fichier config.ini
     *
     * @param configFilePath Chemin vers le fichier de configuration
     */
    private static void loadDatabaseConfig(String configFilePath) {
        Properties properties = new Properties();
        try (FileReader reader = new FileReader(configFilePath)) {
            properties.load(reader);

            JDBC_URL = properties.getProperty("jdbc_url");
            USER = properties.getProperty("user");
            PASSWORD = properties.getProperty("password");

            if (JDBC_URL == null || USER == null || PASSWORD == null) {
                throw new IllegalArgumentException("Le fichier config.ini doit contenir jdbc_url, user et password.");
            }
        } catch (IOException e) {
            
            logger.error("Erreur lors du chargement du fichier de configuration : " + e.getMessage());
            System.exit(1); // Arrête le programme si la configuration ne peut pas être chargée
        }
    }
    
    public static void start() {
        
        logger.info("Service is starting...");
        Runtime.getRuntime().addShutdownHook(new Thread(HyperFileService::stop));
        while (running) {
            execute();
        }
    }

    public static void stop() {

        logger.info("Service is stoping...");
        running = false;
    }
    
    
}