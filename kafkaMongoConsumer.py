
from kafka import KafkaConsumer
from pymongo import MongoClient
import json


def get_word( text ):
    s = text.lower()
    words = ['#mortalkombat', '#tomclancyswithoutremorse', '#armyofthedead',
             '#thelittlethings', '#thunderforce', '#outsidethewire', '#radhe',
             '#thingsheardseen', '#wrathofman', '#drishyam2', '#master',
             '#zacksnydersjusticeleague', '#malcolmmarie', '#tomandjerry',
             '#seaspiracy', '#chaoswalking', '#thosewhowishmedead', '#cruella',
             '#thedig', '#redemptionday', '#themauritanian', '#godzillavskong',
             '#thewomaninthewindow', '#oxygen',
             '#thewhitetiger', '#coming2america', '#palmer',
             '#theconjuring', '#bosslevel', '#nobody',
             '#themitchellsvsthemachines', '#cherry', '#stowaway',
             '#judasandtheblackmessiah', '#nomadland', '#intheheights',
             '#peterrabbit2', '#aquietplace', '#spirituntamed', '#spiral',
             '#thehousenextdoor', '#rayaandthelastdragon', '#dreamhorse',
             '#thosewhowishmedead', '#findingyou', '#thehitman', '#heretoday',
             '#neworder', '#themisfits', '#anotherround','#minari',
             '#thedjinn', '#profile']
    for word in words:
        word = word.lower()
        if word in s:
            return word
    return "none"


try:
    client = MongoClient('localhost',27017)
    db = client.twitter
    print("Connected successfully!!!")
except:
    print("Could not connect to MongoDB")

consumer = KafkaConsumer('streaming',
                         bootstrap_servers=['localhost:9092'])


for msg in consumer:
    record = json.loads(msg.value)
    text = record['text']
    sentiment = record['sentiment'][:4]
    subjectivity = record['subjectivity'][:4]
    location = record['location']
    try:
        twitter_rec = {'text':text,'sentiment':sentiment,'subjectivity':subjectivity,'location':location,'keyword':get_word(text)}
        rec_id1 = db.tweet_info.insert_one(twitter_rec)
        print("Data inserted with record ids",rec_id1)
    except:
        print("Could not insertInMongo")