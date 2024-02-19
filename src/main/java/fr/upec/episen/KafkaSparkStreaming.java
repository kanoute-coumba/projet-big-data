package fr.upec.episen;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.ForeachWriter;
import java.io.IOException;
import java.util.Properties;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.TimeoutException;

public class KafkaSparkStreaming {

    private static Properties props;
    public static void main(String[] args) throws StreamingQueryException, TimeoutException, IOException {
        
        InputStream iStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("application.properties");

            if (iStream == null) {
            throw new RuntimeException("Unable to find or load application.properties");
        }

       props = new Properties();
       props.load(iStream);

        
        SparkConf conf = new SparkConf();
        conf.setAppName("KafkaSparkStructuredStreaming");
        conf.setMaster("local[*]");
        conf.set("spark.sql.sessionCharacterEncoding", "UTF-8");

        SparkSession session = new SparkSession.Builder().config(conf).getOrCreate();
        session.sparkContext().setLogLevel("OFF");

        // Lecture des données depuis Kafka
        Dataset<Row> kafkaDataFrame = session
                .readStream()
                .format("kafka")
                .option("value.deserializer.encoding", "UTF-8")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "topic_CIR")
                .load();

        // Écriture dans la base de données PostgreSQL
        StreamingQuery query = kafkaDataFrame
                .selectExpr("CAST(value AS STRING)")
                .writeStream()
                .outputMode("append")
                .foreach(new ForeachWriter<Row>() {
                    private Connection connection;
                    private PreparedStatement preparedStatement;

                    @Override
                    public boolean open(long partitionId, long version) {
                        // Initialisation de la connexion à la base de données
                        String jdbcUrl = "jdbc:postgresql://localhost:5432/projet-big-data-spark-kafka?charSet=UTF-8";
                        String user = "postgres";
                        String password = "admin";

                        try {
                            connection = DriverManager.getConnection(jdbcUrl, user, password);
                            String query = "INSERT INTO dataset(dispositif, type_structure, annee_agrement, type_organisme, designation, sigle, activite, localisation, debut_agrement, " +
                                           "fin_agrement, numero_siren, categorie_etablissement, codeAPE_etablissement, APE_etablissement, code_postal, ville, " +
                                           "telephone, URL_scanR, commune, unite_urbaine, departement, academie, region, pays, code_commune_fr, code_unite_urbaine_fr, " +
                                           "code_departement_fr, code_academie_fr, code_region_fr, codeISO_pays, geolocalisation, typeI) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                            preparedStatement = connection.prepareStatement(query);
                            return true;
                        } catch (SQLException e) {
                            e.printStackTrace();
                            return false;
                        }
                    }

                    @Override
                    public void process(Row value) {
                        // Logique de traitement pour chaque enregistrement
                        // Convertir la valeur (message) en chaîne de caractères
                        String csvLine = value.getString(0);

                        // Nettoyer la ligne en supprimant les guillemets doubles
                        csvLine = cleanValue(csvLine);
                        //System.out.println (csvLine);

                        // Traitement du message CSV
                        String[] csvValues = csvLine.split(";"); // séparé par point virgule
                        if (csvValues.length >= 32) {
                            try {
                                // On ajuste les colonnes et les valeurs selon votre structure de base de données
                                String dispositif = cleanValue(csvValues.length > 0 ? csvValues[0] : null);
                                String type_structure = cleanValue(csvValues.length > 1 ? csvValues[1] : null);
                                String annee_agrement = cleanValue(csvValues.length > 2 ? csvValues[2] : null);
                                String type_organisme = cleanValue(csvValues.length > 3 ? csvValues[3] : null);
                                String designation = cleanValue(csvValues.length > 4 ? csvValues[4] : null);
                                String sigle = cleanValue(csvValues.length > 5 ? csvValues[5] : null);
                                String activite = cleanValue(csvValues.length > 6 ? csvValues[6] : null);
                                String localisation = cleanValue(csvValues.length > 7 ? csvValues[7] : null);
                                int début_agrement = (csvValues.length > 8 && !csvValues[8].isEmpty()) ? Integer.parseInt(cleanValue(csvValues[8])) : 0;
                                int fin_agrement = (csvValues.length > 9 && !csvValues[9].isEmpty()) ? Integer.parseInt(cleanValue(csvValues[9])) : 0;
                                int numero_siren = (csvValues.length > 10 && !csvValues[10].isEmpty()) ? Integer.parseInt(cleanValue(csvValues[10])) : 0;
                                String categorie_etablissement = cleanValue(csvValues.length > 11 ? csvValues[11] : null);
                                String codeAPE_etablissement = cleanValue(csvValues.length > 12 ? csvValues[12] : null);
                                String APE_etablissement = cleanValue(csvValues.length > 13 ? csvValues[13] : null);
                                String code_postal = cleanValue(csvValues.length > 14 ? csvValues[14] : null);
                                String ville = cleanValue(csvValues.length > 15 ? csvValues[15] : null);
                                String telephone = cleanValue(csvValues.length > 16 ? csvValues[16] : null);
                                String URL_scanR = cleanValue(csvValues.length > 17 ? csvValues[17] : null);
                                String commune = cleanValue(csvValues.length > 18 ? csvValues[18] : null);
                                String unite_urbaine = cleanValue(csvValues.length > 19 ? csvValues[19] : null);
                                String departement = cleanValue(csvValues.length > 20 ? csvValues[20] : null);
                                String academie = cleanValue(csvValues.length > 21 ? csvValues[21] : null);
                                String region = cleanValue(csvValues.length > 22 ? csvValues[22] : null);
                                String pays = cleanValue(csvValues.length > 23 ? csvValues[23] : null);
                                String code_commune_fr = cleanValue(csvValues.length > 24 ? csvValues[24] : null);
                                String code_unite_urbaine_fr = cleanValue(csvValues.length > 25 ? csvValues[25] : null);
                                String code_departement_fr = cleanValue(csvValues.length > 26 ? csvValues[26] : null);
                                String code_academie_fr = cleanValue(csvValues.length > 27 ? csvValues[27] : null);
                                String code_region_fr = cleanValue(csvValues.length > 28 ? csvValues[28] : null);
                                String codeISO_pays = cleanValue(csvValues.length > 29 ? csvValues[29] : null);
                                String geolocalisation = cleanValue(csvValues.length > 30 ? csvValues[30] : null);
                                String typeI = cleanValue(csvValues.length > 31 ? csvValues[31] : null);
                                    
                                // Ajustez les paramètres du PreparedStatement
                                preparedStatement.setString(1, dispositif);
                                preparedStatement.setString(2, type_structure);
                                preparedStatement.setString(3, annee_agrement);
                                preparedStatement.setString(4, type_organisme);
                                preparedStatement.setString(5, designation);
                                preparedStatement.setString(6, sigle);
                                preparedStatement.setString(7, activite);
                                preparedStatement.setString(8, localisation);
                                preparedStatement.setInt(9, début_agrement);
                                preparedStatement.setInt(10, fin_agrement);
                                preparedStatement.setInt(11, numero_siren);
                                preparedStatement.setString(12, categorie_etablissement);
                                preparedStatement.setString(13, codeAPE_etablissement);
                                preparedStatement.setString(14, APE_etablissement);
                                preparedStatement.setString(15, code_postal);
                                preparedStatement.setString(16, ville);
                                preparedStatement.setString(17, telephone);
                                preparedStatement.setString(18, URL_scanR);
                                preparedStatement.setString(19, commune);
                                preparedStatement.setString(20, unite_urbaine);
                                preparedStatement.setString(21, departement);
                                preparedStatement.setString(22, academie);
                                preparedStatement.setString(23, region);
                                preparedStatement.setString(24, pays);
                                preparedStatement.setString(25, code_commune_fr);
                                preparedStatement.setString(26, code_unite_urbaine_fr);
                                preparedStatement.setString(27, code_departement_fr);
                                preparedStatement.setString(28, code_academie_fr);
                                preparedStatement.setString(29, code_region_fr);
                                preparedStatement.setString(30, codeISO_pays);
                                preparedStatement.setString(31, geolocalisation);
                                preparedStatement.setString(32, typeI);

                                // Exécution de la requête
                               // preparedStatement.executeUpdate();
                            } catch (SQLException e) {
                                e.printStackTrace();
                            }
                        } else {
                            System.err.println("Format CSV invalide: " + csvLine);
                        }
                    }

                    @Override
                    public void close(Throwable errorOrNull) {
                        // Fermeture de la connexion à la base de données
                        try {
                            if (preparedStatement != null) {
                                preparedStatement.close();
                            }
                            if (connection != null) {
                                connection.close();
                            }
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                })
                .trigger(Trigger.ProcessingTime("10 seconds"))
                .start();

        query.awaitTermination(30000); 
       
        Dataset<Row> reader = session.read()
                .format("jdbc")
                .option("url", "jdbc:postgresql://localhost:5432/projet-big-data-spark-kafka")
                .option("dbtable", "dataset")
                .option("user", "postgres")
                .option("password", "admin")
                .load();

        // 5. Créer une vue temporaire à partir du Dataset
        reader.createOrReplaceTempView("dataset");
      
                        // Obtenez la date actuelle
        Date currentDate = new Date();

        // Définissez le format de l'horodatage que vous souhaitez utiliser
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");

        // Formatez la date actuelle selon le format défini
        String timestamp = dateFormat.format(currentDate);

        //////////////////////////// Requete 1 ////////////////////////////////
        System.out.println("Avant l'exécution de la requête 1");
        
        //String request1 = "SELECT annee_agrement, departement, COUNT(*) AS Nombre_Agrements FROM dataset WHERE annee_agrement LIKE '%2015%' GROUP BY annee_agrement, departement ORDER BY Nombre_Agrements DESC;";
        final String request1 = props.getProperty("request1");
        Dataset<Row> result1 = session.sql(request1);
        System.out.println("Après l'exécution de la requête request 1");
        result1.show(30000,false);
        System.out.println("Résultats affichés avec succès request 1");  

        
                // Spécifiez le chemin d'enregistrement du fichier CSV
        String outputCSVPath1 = "C:/Users/kanou/Desktop/programmes/workspaceIng3FI/projet-big-data/src/main/resources/result1"+ timestamp +".csv";

        // Utilisez la méthode write pour écrire le DataFrame au format CSV
        result1.write()
                .mode("overwrite") 
                .option("header", "true")
                .csv(outputCSVPath1);

     
        //////////////////////////// Requete 2 ////////////////////////////////
        System.out.println("Avant l'exécution de la requête 2");
        //String request2 = "SELECT activite, departement, COUNT(*) AS Nombre_Agrements FROM dataset WHERE activite = 'Chimie' AND Pays = 'France' GROUP BY activite, departement ORDER BY Nombre_Agrements DESC;";
        final String request2 = props.getProperty("request2");
        Dataset<Row> result2 = session.sql(request2);
        System.out.println("Après l'exécution de la requête request 2");
        result2.show(30000,false);
        System.out.println("Résultats affichés avec succès request 2");  
        
        String outputCSVPath2 = "C:/Users/kanou/Desktop/programmes/workspaceIng3FI/projet-big-data/src/main/resources/result2"+ timestamp +".csv";

        // Utilisez la méthode write pour écrire le DataFrame au format CSV
        result2.write()
                .mode("overwrite") 
                .option("header", "true")
                .csv(outputCSVPath2);

        }
    
    
    // Méthode pour nettoyer la valeur ou la ligne
    private static String cleanValue(String value) {
        if (value != null) {
            // Supprimer les guillemets doubles indésirables
            value = value.replaceAll("\"", "");
        }
        return value;

    }    
    
}