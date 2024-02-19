from kafka import KafkaProducer
import json

# Configuration de Kafka
kafka_bootstrap_servers = 'localhost:9092'  # adresse du serveur Kafka
kafka_topic = 'topic_CIR'

# Chemin vers le fichier exporté au format CSV
data_file_path = 'C:/Users/kanou/Desktop/programmes/workspaceIng3FI/projet-big-data/src/main/resources/cir.csv'  # Remplacez cela par le chemin réel du fichier

def read_data(file_path, encoding='utf-8'):
    # Lire les données depuis le fichier (adapter cette fonction selon le format du fichier)
    with open(file_path, 'r', encoding=encoding) as file:
        # Exclure la première ligne (entête) en utilisant [1:]
        data = file.readlines()[1:]
    return data

def publish_data_to_kafka(data):
    # Publier chaque ligne du fichier dans le topic Kafka
    producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers,
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    for line in data:
        # Supprimez le caractère de saut de ligne à la fin de chaque ligne
        line = line.rstrip('\n')
        producer.send(kafka_topic, value=line)

    producer.flush()
    producer.close()

if __name__ == '__main__':
    # Lecture des données depuis le fichier avec l'encodage utf-8
    data = read_data(data_file_path, encoding='utf-8')

    # Affichage du message avant la publication dans Kafka
    print("Les données seront publiées dans le topic Kafka.")

    # Publication des données dans le topic Kafka
    publish_data_to_kafka(data)

    # Message pour indiquer que la publication est terminée
    print("Les données ont été publiées dans le topic Kafka.")
