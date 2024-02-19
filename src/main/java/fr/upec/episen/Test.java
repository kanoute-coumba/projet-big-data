package fr.upec.episen;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.ForeachWriter;

import java.util.concurrent.TimeoutException;

public class Test {

    public static void main(String[] args) throws StreamingQueryException, TimeoutException {

        SparkConf conf = new SparkConf();
        conf.setAppName("KafkaSparkStructuredStreaming");
        conf.setMaster("local[*]");

        SparkSession session = new SparkSession.Builder().config(conf).getOrCreate();
        session.sparkContext().setLogLevel("OFF");

        // Lecture des données depuis Kafka
        Dataset<Row> kafkaDataFrame = session
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "sujet1")
                .load();

        // Écriture dans la base de données PostgreSQL
        StreamingQuery query = kafkaDataFrame
                .selectExpr("CAST(value AS STRING)")
                .writeStream()
                .outputMode("append")
                .foreach(new ForeachWriter<Row>() {
                    private java.sql.Connection connection;
                    private java.sql.Statement statement;

                    @Override
                    public boolean open(long partitionId, long version) {
                        // Initialisation de la connexion à la base de données
                        String jdbcUrl = "jdbc:postgresql://localhost:5432/test";
                        String user = "postgres";
                        String password = "admin";

                        try {
                            connection = java.sql.DriverManager.getConnection(jdbcUrl, user, password);
                            statement = connection.createStatement();
                            return true;
                        } catch (java.sql.SQLException e) {
                            e.printStackTrace();
                            return false;
                        }
                    }

                    @Override
                    public void process(Row value) {
                        // Logique de traitement pour chaque enregistrement
                        // Convertir la valeur (message) en chaîne de caractères
                        String csvLine = value.getString(0);

                        // Traitement du message CSV
                        String[] csvValues = csvLine.split(","); // Supposant une séparation par virgule
                        if (csvValues.length >= 2) {
                            // On s'asssure que la structure de votre CSV correspond à vos besoins
                            int idValue = Integer.parseInt(csvValues[0]);
                            String nameValue = csvValues[1];

                            // On ajuste les colonnes et les valeurs selon votre structure de base de données
                            String query = String.format("INSERT INTO guest(id, name) VALUES (%s, '%s')", idValue, nameValue);

                            // Exécution de la requête
                            try {
                                statement.executeUpdate(query);
                            } catch (java.sql.SQLException e) {
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
                            if (statement != null) {
                                statement.close();
                            }
                            if (connection != null) {
                                connection.close();
                            }
                        } catch (java.sql.SQLException e) {
                            e.printStackTrace();
                        }
                    }
                })
                .trigger(Trigger.ProcessingTime("10 seconds"))
                .start();

        query.awaitTermination();
    }
}
