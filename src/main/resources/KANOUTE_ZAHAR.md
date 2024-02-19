Compte rendu projet Big Data - Spark - Kafka

1. L'ARCHITECTURE
Pour les besoins du projet, on a besoin d'une architecture avec Apache Spark avec un cluster Kafka, le tout installé en local, donc Localhost.
On a choisi de mettre l'achitecture de Kafka sur un systeme Windows, Spark étant déjà installé aussi nos PCs
- Fichiers de configuration .properties de zookeeper et server du dossier kafka/config/ et y mettre respectivement aux lignes dataDir=C:/kafka_2.13-3.6.0/tmp/zookeeper et log.dirs=C:/kafka_2.13-3.6.0/tmp/kafka-logs afin de specifier le répertoire pour les fichiers de journaalisation
- Lancer respectivement zookeeper et kafka
    .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
    .\bin\windows\kafka-server-start.bat .\config\server.properties
- Créer notre topic
    .\kafka-topics.bat --create --bootstrap-server localhost:9092 --topic topic_CIR
- Créer un producer
    .\kafka-console-producer.bat --topic topic_CIR --bootstrap-server localhost:9092
- Créer un consumer
    .\kafka-console-consumer.bat --topic topic_CIR --bootstrap-server localhost:9092 --from-beginning

On peut tester que l'architecture fonctionne en envoyant des messages depuis le producer dans le topic et on les reçoit chez le consumer. On a egalement fait un petit consumer Java Spark qui lit les messages depuis le producer.
On a créé en même temps une connexion vers une base de données Postgres afin de valider l'hypothèse que les données sont bien reçues

2. L'EXTRACTION
Les données proviennent de l'open data du MESR (Ministère de l'Enseignement Supérieur et de la
Recherche). Elles ont été téléchargées au format CSV.
Pour extraire les données de cette source, nous avons créé un programme python qui sera donc notre producer. Ce script va jouer maintenant le role du producer en prenant le CSV téléchargé et y extraire ligne par ligne tous les enregistrments qu'il contient.

3. LE STREAMING
Par la suite, nous avons créé le consumer Java Spark qui lit et enregistre les messages en stream dans une base de données Postgres. Il vous sera montré en démo que la base de données est vide. Ensuite nous allons executer le script python, attendre que les données soient complètement chargées. Puis montrer que les enregistrements ont été sauvegardés en base.

4. LES REQUETES
Maintenant que l'on dispose des données dans la base, il s'agit d'executer des requetes SQL. La configuration et les requetes sont ecrites dans un fichier application.properties. Apres l'execution des requetes, leurs resultats sont imprimés à l'écran mais aussi exportés dans des fichiers en sortie CSV afin de pouvoir effectuer des visualisations

request1 = SELECT annee_agrement, departement, COUNT(*) AS Nombre_Agrements FROM dataset WHERE annee_agrement LIKE '%2015%' AND Pays = 'France' GROUP BY annee_agrement, departement ORDER BY Nombre_Agrements DESC;

request2 = SELECT activite, departement, COUNT(*) AS Nombre_Agrements FROM dataset WHERE activite = 'Chimie' AND Pays = 'France' GROUP BY activite, departement ORDER BY Nombre_Agrements DESC;

5. LA VISUALISATION
Pour pouvoir visualiser les données dans Gephi, nous avons d'abord importé les fichiers en output du programme Spark dans l'outil Gephi.







