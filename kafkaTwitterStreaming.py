
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import json
import pykafka
from textblob import TextBlob


class TweetListener(StreamListener):
    def __init__(self):
        self.client = pykafka.KafkaClient("localhost:9092")
        self.producer = self.client.topics[bytes('streaming','ascii')].get_producer()
    def on_data(self, data):
        try:
            json_data = json.loads(data)
            send_data = '{}'
            json_send_data = json.loads(send_data)			

            if "retweeted_status" in json_data:
                try:
                    json_send_data['text'] = json_data['retweeted_status']['extended_tweet']['full_text']
                except:
                    json_send_data['text'] = json_data['retweeted_status']['text']
            else:
                try:
                    json_send_data['text'] = json_data['extended_tweet']['full_text']  
                except:
                    json_send_data['text'] = json_data['text']

            json_send_data['location'] = json_data['user']['location'] 


            blob = TextBlob(json_send_data['text'])
            (json_send_data['sentiment'], json_send_data['subjectivity']) = blob.sentiment

            if json_send_data['sentiment'] == 1.6653345369377347e-17:
                json_send_data['sentiment']=0
            json_send_data['sentiment'] = str(json_send_data['sentiment'])[:4]
            json_send_data['subjectivity'] = str(json_send_data['subjectivity'])[:4]
            print(json_send_data['text'], " >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> ", json_send_data['sentiment'])

            self.producer.produce(bytes(json.dumps(json_send_data),'ascii'))
            return True
        except KeyError:
            return True
        
    def on_error(self, status):
        print(status)
        return True



if __name__ == "__main__":
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
             '#neworder', '#themisfits', '#anotherround', '#minari',
             '#thedjinn', '#profile']

    consumer_key = 'oOngHbyErzDIMqVT4Pk1MYeWY'
    consumer_secret = 'j9d8jx7DnNbKo3kUnEV5swUMzTdZtyegBbsS505C0D832zr2af'
    access_token = '769548315218284544-8uq8V1LXL2mUkkJz9vqQEHqBwLdRQkl'
    access_secret = 'oUIYxszRh1dJUfTu2KaEPq7YCRJb3g3AChxpb9wf7VcoG'

    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)

    twitter_stream = Stream(auth, TweetListener(), tweet_mode='extended')
    twitter_stream.filter(languages=["en"],track=words)
