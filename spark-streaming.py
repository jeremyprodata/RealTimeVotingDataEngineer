import pyspark 
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import from_json, col




if __name__ == "__main__":
    # verifions que spark est bien installé 
    # print(pyspark.__version__) # Je dois utiliser la version pyspark 3.5.0

    # Initialisons notre session spark
    spark = (SparkSession.builder
             .appName("RealtimeVotingEngineering")
             # Spark kafka integration
             .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1') # source de ces informations: https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10_2.13/4.0.1
 
             # Nous travaillons avec postgresql, nous devons donc ajouter le driver jdbc postgresql     
             # Ce connecteur nous parmettra de nous connecter à notre base de données postgresql et communiquer en tps réel avec lui.
             # Pour cela rdv sur https://jdbc.postgresql.org/ et vous trouverez le pilote jdbc postgresql. 
             # On va le télécharger 
             .config('spark.jars', '\Users\jerey\Desktop\Projet_Entretient\RealTimeVotingEngineering\postgresql-42.7.9.jar') # postgresql driver

             # l'étape suivante va suivante consiste à spécifier la désactivation adaptative.
             # On est pas de le faire explicitement, car par defaut, elle sera probablement désactivée.
             .config('spark.sql.adaptive.enabled', 'false') # disable adaptative query execution
             .getOrCreate() # Cela va créer une session spark et je pourrai utiliser cette variable spark comme contexte spark lors de l'exécution de mon code.
             )
    
    # Maintenant que notre variable spark a été créée via la configuration Spark Builder.
    # Je dois dupliquer les données provenant du topics dans un schémas particulier.  
    # Nous allons donc spécifier ce schéma.
    # Define schemas for Kafka topics
    vote_schema = StructType([
        StructField("voter_id", StringType(), True),
        StructField("candidate_id", StringType(), True),
        StructField("voting_time", TimestampType(), True),
        StructField("voter_name", StringType(), True),
        StructField("party_affiliation", StringType(), True),
        StructField("biography", StringType(), True),
        StructField("campaign_platform", StringType(), True),
        StructField("photo_url", StringType(), True),
        StructField("candidate_name", StringType(), True),
        StructField("date_of_birth", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("nationality", StringType(), True),
        StructField("registration_number", StringType(), True),
        StructField("address", StructType([
            StructField("street", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("country", StringType(), True),
            StructField("postcode", StringType(), True)
        ]), True),
        StructField("email", StringType(), True),
        StructField("phone_number", StringType(), True),
        StructField("cell_number", StringType(), True),
        StructField("picture", StringType(), True),
        StructField("registered_age", IntegerType(), True),
        StructField("vote", IntegerType(), True)
    ])


    # Read data from Kafka 'votes_topic' and process it
    votes_df = (spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "votes_topic")
                .option("startingOffsets", "earliest") # récupére les données de la fonction earliest 
                .load()
                .selectExpr("CAST(value AS STRING)") # puis le converti en chaine de carac
                .select(from_json(col("value"), vote_schema).alias("data")) # les converti en json
                .select("data.*") # et selectionne tt ce qu'y s'y trouve
                )
    
    # Ensuite on va convertir l'horodatage du vote en 



    ##############################                         1H14min

    


    
    

    


