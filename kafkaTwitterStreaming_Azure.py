from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import Stream
import json
import pykafka
# from afinn import Afinn
# from langdetect import detect
from textblob import TextBlob
from azure.ai.textanalytics import TextAnalyticsClient
from azure.core.credentials import AzureKeyCredential


# initialize twitter producer class, inheriting from StreamListener class
class TweetListener(Stream):

    # kafka client and topic
    def __init__(self):

        self.client = pykafka.KafkaClient("localhost:2181")
        self.producer = self.client.topics[bytes('energy', 'ascii')].get_producer()

    def authenticate_client(self): # azure portal authentication
        key = "paste-your-key-here"
        endpoint = "paste-your-endpoint-here"

        ta_credential = AzureKeyCredential(key)
        text_analytics_client = TextAnalyticsClient(
            endpoint=endpoint,
            credential=ta_credential)
        return text_analytics_client

    # parse json tweet object stream to get desired data
    def on_data(self, data):
        try:
            json_data = json.loads(data)
            send_data = '{}'
            json_send_data = json.loads(send_data)

            # make checks for retweet and extended tweet-->done for truncated text
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

            json_send_data['creation_datetime'] = json_data['created_at']
            json_send_data['username'] = json_data['user']['name']
            json_send_data['location'] = json_data['user']['location']
            json_send_data['userDescr'] = json_data['user']['description']
            json_send_data['followers'] = json_data['user']['followers_count']
            json_send_data['retweets'] = json_data['retweet_count']
            json_send_data['favorites'] = json_data['favorite_count']

            # language detection and use of appropriate sentiment analysis module
            ###blob = TextBlob(json_send_data['text'])
            ### if blob.detect_language() == 'en':
            ### (json_send_data['senti_val'], json_send_data['subjectivity']) = blob.sentiment
            ###elif blob.detect_language() == 'nl':
            ### blob = blob.translate(to='en')
            ### (json_send_data['senti_val'], json_send_data['subjectivity']) = blob.sentiment
            # check for this really small value and make it 0
            ###if json_send_data['senti_val'] == 1.6653345369377347e-17:
            ### json_send_data['senti_val'] = 0
            # keep only the first two decimal points of sentiment value and subjectivity

            client = self.authenticate_client()
            (sentiment, subjectivity) = client.analyze_sentiment(json_send_data['text']) # calling sentiment analysis method of azure congnetive services for languages
            json_send_data['senti_val'] = str(sentiment)
            json_send_data['subjectivity'] = str(subjectivity)

            ###json_send_data['senti_val'] = str(json_send_data['senti_val'])[:4]
            ###json_send_data['subjectivity'] = str(json_send_data['subjectivity'])[:4]

            # printing for testing
            # print(json_data)
            print(json_send_data)
            # print(json_send_data['location'])
            # print(json_send_data['text'], " >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> ", json_send_data['senti_val'])

            # push data to producer
            self.producer.produce(bytes(json.dumps(json_send_data), 'ascii'))
            return True
        except KeyError:
            return True

    def on_error(self, status):
        print(status)
        return True


if __name__ == "__main__":
    # keywords to look for: commas work as or, words in the same phrase work as and
    words = ['netherlands energy', 'nederland energie', 'netherlands gas-free', 'netherlands gas free',
             'nl gas free', 'nl gas-free', 'energie transitie', 'Groningen gas', 'energy transition netherlands',
             'groningen aardbevingen', 'groningen gas-drilling', 'groningen earthquakes', 'netherlands sustainable',
             'nederland groningen', 'nederland gas boren', 'mark rutte energy', 'mark rutte energie',
             'CO-2 reductie Nederland']

    # twitter api credentials
    consumer_key = 'n6TdQ7qSpjJkMUjeOG0SDZUGW'
    consumer_secret = 'PgBIu35oI3iVfeXbPNDJ7h63uqlYIFRQbO38F0HHXV4WYr8Z63'
    access_token = '2511047768-SRyZ0eCZiai5o1q8r49QXb8z8hcozMfqoq6lU0a'
    access_secret = 'nB2GD6nmiho12JuwQt1yQx2Jy61W3TLs7FvvD4L99vitG'

    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
dsa
    # create AFINN object for sentiment analysis
    # afinn = Afinn(emoticons=True)

    # perform activities on stream
    twitter_stream = Stream(auth, TweetListener(), tweet_mode='extended')
    twitter_stream.filter(track=words)
