import psycopg2
import requests
import random
import simplejson as json
from confluent_kafka import SerializingProducer

BASE_URL = 'https://randomuser.me/api/?nat=gb'
PARTIES = ['Management_Party', 'Savior_Party', 'Tech_Republic_Party']

# Assurons nous de générer des données, afin d'avoir qqch d'unique à chaque éxécution
# et que le meme enregistrement ne soit pas retourné, nous allons utiliser un nbr aléatoire
random.seed(21)


def create_tables(conn, cur):
    # C'est ici que nous allons établir notre connexion à la base de données et créer la table 
    # on utilis ela méthode execute du cursor pour exécuter la requete sql dans la base données 
    cur.execute("""
        CREATE TABLE IF NOT EXISTS candidates (
            candidate_id VARCHAR(255) PRIMARY KEY,
            candidate_name VARCHAR(255),
            party_affiliation VARCHAR(255),
            biography TEXT,
            campaign_platform TEXT,
            photo_url TEXT
        )
    """)
    # Creation de la table des élécteurs
    cur.execute("""
        CREATE TABLE IF NOT EXISTS voters (
            voter_id VARCHAR(255) PRIMARY KEY,
            voter_name VARCHAR(255),
            date_of_birth VARCHAR(255),
            gender VARCHAR(255),
            nationality VARCHAR(255),
            registration_number VARCHAR(255),
            address_street VARCHAR(255),
            address_city VARCHAR(255),
            address_state VARCHAR(255),
            address_country VARCHAR(255),
            address_postcode VARCHAR(255),
            email VARCHAR(255),
            phone_number VARCHAR(255),
            cell_number VARCHAR(255),
            picture TEXT,
            registered_age INTEGER
        )
    """)

    # Creation de la table des votes 
    # La clé primaire est une clé composite composé de l'id de l'electeur et de l'id candidat afin de garantir qu'il n'y ai qu'un seul vote par candidat 
    # L'élécteur doit egalement etre unique, c'et pourquoi on a ajouter une contraite d'unicité 
    cur.execute("""
        CREATE TABLE IF NOT EXISTS votes (
            voter_id VARCHAR(255) UNIQUE,
            candidate_id VARCHAR(255),
            voting_time TIMESTAMP,
            vote int DEFAULT 1,
            PRIMARY KEY (voter_id, candidate_id)
        )
    """)

    # Maintenant, il ne nous reste plus qu'à effectuer la connexion 
    conn.commit()



def generate_candidate_data(candidate_number, total_parties):
    # Nous allons obtenir les information du candidats d'un utilisateurs aléatoire 
    # nous allons faire une requete get à l'api random user
    response = requests.get(BASE_URL + '&gender=' + ('femal' if candidate_number %2 == 1 else 'male'))
    if response.status_code == 200: #????? 200 ????
        # je vais générer les données utilisateur et dans user_data à partir de response qu'on va convertir en json 
        user_data = response.json()['results'][0] #cela nous donne les données utilisateur 
        
        # et je demande simplement de retourner: 
        return {
            "candidate_id": user_data['login']['uuid'],
            "candidate_name": f"{user_data['name']['first']} {user_data['name']['last']}",
            "party_affiliation": PARTIES[candidate_number % total_parties],
            "biography": "A brief bio of the candidate.",
            "campaign_platform": "Key campaign promises or platform.",
            "photo_url": user_data['picture']['large']
        }
    else:
        return "Error fetching data" 




def generate_voter_data():
    response = requests.get(BASE_URL)
    if response.status_code == 200:
        user_data = response.json()['results'][0]
        return {
            "voter_id": user_data['login']['uuid'],
            "voter_name": f"{user_data['name']['first']} {user_data['name']['last']}",
            "date_of_birth": user_data['dob']['date'],
            "gender": user_data['gender'],
            "nationality": user_data['nat'],
            "registration_number": user_data['login']['username'],
            "address": {
                "street": f"{user_data['location']['street']['number']} {user_data['location']['street']['name']}",
                "city": user_data['location']['city'],
                "state": user_data['location']['state'],
                "country": user_data['location']['country'],
                "postcode": user_data['location']['postcode']
            },
            "email": user_data['email'],
            "phone_number": user_data['phone'],
            "cell_number": user_data['cell'],
            "picture": user_data['picture']['large'],
            "registered_age": user_data['registered']['age']
        }
    else:
        return "Error fetching data"




def insert_voters(conn, cur, voter):
    cur.execute("""
                        INSERT INTO voters (voter_id, voter_name, date_of_birth, gender, nationality, registration_number, address_street, address_city, address_state, address_country, address_postcode, email, phone_number, cell_number, picture, registered_age)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s)
                        """,
                (voter["voter_id"], voter['voter_name'], voter['date_of_birth'], voter['gender'],
                 voter['nationality'], voter['registration_number'], voter['address']['street'],
                 voter['address']['city'], voter['address']['state'], voter['address']['country'],
                 voter['address']['postcode'], voter['email'], voter['phone_number'],
                 voter['cell_number'], voter['picture'], voter['registered_age'])
                )
    conn.commit()


def delivery_report(err, msg):
    if err is not None:
        print(f"Messge delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")






if __name__ == "__main__":
    
    producer = SerializingProducer({'bootstrap.servers': 'localhost:9092'})



    #Essayons de nous connecter à un serveur postgresql
    #Utilisons un bloc try/except pour gérer les erreurs
    try: 
        conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
        # Placons maintenat notre curseur dans le systeme
        # Le curseur va nous permettre de nous connecter à Postgresql, de créer des tables, d'exécuter des requetes et d'obtenir des résultats  
        cur = conn.cursor()


        # La première chose à faire est de créer la table. Nous allons utiliser la fonction create_tables en lui passant en argument la connexion et le curseur 
        create_tables(conn, cur)

        # Une fois nos table créé, on peut alors continuer 
        # Pour pouvoir voter il faut récupérer le candidat pour lequel on veut voter 
        # il faut donc créer ce candidat dans le système 
        # Ainsi la première étape va consister à récupérer le candidat ainsi que la partis pour lequel les élécteurs voteront. 

        # Récupérons tous les candidat présents dans le système
        cur.execute("""
            SELECT * FROM candidates;
                    """)
        
        # La commande suivante récupère tos les candidats présents dans le système
        candidates = cur.fetchall()

        # Si le candidats n'a jamais été enregistré auparavant et que la longueur du champ "candidates" est égale à 0,
        # cela signifie qu'aucun candidat n'est enregistré dans le système 
        if len(candidates) == 0:
            # inscrivons donc un nouveau candidat (dans notre cas on va inscrire 3 nouveau candidats)
            for i in range(3):
                # on va définire un générateur de candidat 
                # cette logique pourais etre géré coté interface utilisateur, ou le candidats pour s'inscrire en tant qu'utilisateur
                # mais nous allons tout gérer coté serveur 
                candidate = generate_candidate_data(i, 3)
                # Une fois que nous avons généré notre nouveau candidat
                # l'étape suivante va consister à insérer ce candidat dans la BD postgresql
                cur.execute("""
                        INSERT INTO candidates (candidate_id, candidate_name, party_affiliation, biography, campaign_platform, photo_url)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                candidate['candidate_id'], candidate['candidate_name'], candidate['party_affiliation'], candidate['biography'],
                candidate['campaign_platform'], candidate['photo_url']))
            conn.commit()




        ### Jusqu'ici tt fonctionne: j'ai mes 3 candidat pour l'éléction
        """
        L'étape suivante va consister à générer des information sur nos élécteurs.
        Nous allons utiliser 1000 élécteur comme échantillion de base.
        A noter: traduction de voter en francais c'est élécteur
        """
        for i in range(1000):
            # on commance par générer un élécteur à l'aide de la fonction génératrice créé pour cela 
            voter_data = generate_voter_data()
            # une fois notre élécteur généré, nous allons l'insérer dans la table voter de notre bd postres 
            # en utilisant la connexion, le curseur et les données de l'élécteur qu'on vientd de générer 
            insert_voters(conn, cur, voter_data)

            # La prochaine étape va consister à intégrer notre producer kafka 
            # Nous allons écrire dans notre broker kafka un message 
            producer.produce(
                "voter_topic",
                key=voter_data['voter_id'],
                value=json.dumps(voter_data),
                # Ceci est facultatif, on peut l'ajouter si on veut savoir si nos données sont bien livré ou non 
                on_delivery=delivery_report

            )


            print('Producer voter {}, data:{}'.format(i, voter_data))

            # On s'assure que toutes les données sont bien livrées
            producer.flush()














    # Gérons l'erreur si la connection au serveur postgresql echoue     
    except Exception as e:
        print(e)
