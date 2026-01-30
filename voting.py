import psycopg2
from confluent_kafka import Consumer, KafkaError, KafkaException, SerializingProducer
import simplejson as json
import random 
from datetime import datetime 
from main import delivery_report







# Configuration de notre consumer 
conf = {
    'boostrap.servers':'localhost:9092'
}
consumer = Consumer(conf | {
    'group.id': 'voting-group',
    'auto.offset.reset': 'earLiest',
    'enable.auto.commit': False
})
# OK nous avon donc un consumer

# Nous avons aussi notre produceur 
produceur = (conf)








"""
Nous avons inscrit les élécteur et les candidats pour l'éléction.
L'étape suivante consiste donc à lancer le processus de vote.
Une fois le vote lancé, nous n'aurons plus besoin d'accréditer les élécteurs et aucun nouvel élécteur sera inscrit. 
Ce que nous voulons faire désormais c'est lancer le processus de vote pour ces élécteurs accrédités. 
Ils voteront sur la plateforme. Commencons donc à créer notre plateforme de vote, 


"""

if __name__ == "__main__":
    # Récupérons la connéxion à la BD postgres
    conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
    cur = conn.cursor()
    # Nous avons pu nous connecter à notre BD postres, l'étape suivant va consister à récupérer nos candidat de la BD avec une requete SQL
    candidate_query = cur.execute(
        """
        SELECT row_to_json(col)
        FROM(
            SELECT * FROM candidates
        ) col;
        """
    )
    # La commande suivante récupère tos les candidats présents dans le système
    #candidates = cur.fetchall()
    #print(candidates) # <--- Ici on test si on à bien récupérer nos 3 candidat, on voit qu'on les a récupérer dans une liste de tuple (à mon avis)
    # Le problème c'est qu'on récupères les valeurs des candidats mais pas les titres des colonnes, donc on sait pas à quoi correspod ces valeurs.
    # Pour résoudre ce problème de mappage de donné nous allons utiliser une fonction sql row_to_json()
    # On récupére alors les candidats dans un tableau de tuple de json
    # On va assayer de sortir nos candidat dans un format plus jolie 
    # comme une liste de json par example
    candidates = [candidate[0] for candidate in cur.fetchall()]
    #print(candidates)

    if len(candidates) == 0: 
        raise Exception("No candidates found in the database")
    else:
        print(candidates)


    # A l'aide de notre consumer nous allons nous abonner à un topic particulier que nous avons créer dans le fichier main.py, voters_topic
    consumer.subscribe(['voters_topic'])
    # On est dans le voters_topic 
    try: 
        while True:
            # consumer.poll(...) demande au consumer : “as-tu un message pour moi ?”
            # timeout=1.0 signifie : attendre au maximum 1 seconde
            # Si un message arrive pendant ce temps → il est retourné dans msg
            # Sinon → msg vaut généralement None
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            elif msg.error():
                if msg.error().code() == KafkaError.PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break
            else:
                # Si il un élécteur dans msg je le récupère
                voter = json.loads(msg.value().decode('utf-8'))
                # Maintenant que j'ai mes candidat et mon élécteur il peut voter pour un des candidats
                # je vais donc récupérer sont vote 
                chosen_candidate = random.choice(candidates)
                vote = voter | chosen_candidate | {
                    'voting_time': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                    'vote': 1
                }


#########################  FFFFFFFFFFIIIIIIIIIINNNNNNNNNNNIIIIIIIIIIIRRRRRRRRRR, j'en suis à la 56 min et 30 seconde 


                try:
                    print('User {} is voting for candidate: {}'.format(vote['voter_id'], vote['candidate_id']))
                    # Insérons ce vote dans la base de données postgres
                    cur.execute("""
                            INSERT INTO votes (voter_id, candidate_id, voting_time)
                            VALUES (%s, %s, %s)
                        """, (vote['voter_id'], vote['candidate_id'], vote['voting_time']))
                    conn.commit()



                    """
                    Une fois le vote inséré dans la BD nous devons effectuer un producer dans le topics.
                    C'est important d'insérer le vote dans la BD avant de produire dans le topics, car si l'insertion échou échoue 
                    a cause d'une violiation de l'unicité dans la BD. Un meme candidat qui vote plusieurs fois. 
                    On evitera de produire le vote non conforme dans le topic. 

                    """

                    produceur.produce(
                        'votes_topic',
                        key=vote['voter_id'],
                        value=json.dumps(vote),
                        on_delivery=delivery_report 
                    )
                    produceur.poll(0)


                except Exception as e:
                    print('Error', e)

                

                

    except Exception as e:
        print(e)


