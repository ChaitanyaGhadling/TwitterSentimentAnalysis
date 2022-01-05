import tweepy
import socket
import re
from tweepy.streaming import StreamListener
from tweepy import API
from tweepy import OAuthHandler




class TwitterStreamListener(StreamListener):
        
        def __init__(self, sc=None):
                super(TwitterStreamListener, self).__init__()
                self.max_tweets = 20
                self.tweet_count =0
                self.client_socket = sc

        def on_status(self, status):
                try:
                    status
                except TypeError:
                    print("completed")
                else:
                    self.tweet_count = 1
                    if(self.tweet_count == self.max_tweets):
                        print("completed")
                        return (False)
                    else:
                        tweet = self.get_tweet(status)
                        self.client_socket.send((tweet[2]+"\n").encode('utf-8'))
                return True
            
        def on_error(self, status_code):
                print("Status code")
                print(status_code)
                if status_code == 420:
                        return False

        def get_tweet(self,tweet):
                text = tweet.text
                if hasattr(tweet, 'extended_tweet'):
                        text = tweet.extended_tweet['full_text']
                return [str(tweet.user.id),tweet.user.screen_name,self.clean_str(text)]

        def clean_str(self, string):
                string = re.sub(r"\n|\t", " ", string)
                return string

if __name__ == '__main__':
        consumer_key = 'orh7UbxDKBMs5Po0C3aJOBlBo'
        consumer_secret = 'Lo6h8BAbt2rI1p4GhwjYCiVBqWSFGADWQr0BNcaw5Bs0xFD7Nw'
        access_token = '1458837149915226133-qC9PO7PBZgfelJTWuEpUFLookNHhzm'
        access_secret = 'oPLklgHa8CZk8tHRjUT7WlFEMfopK9LD2mMSTsrxvdQEY'

        # Local connection
        host = "127.0.0.1"          
        port = 5518                 # Reserve a port for your service.

        s = socket.socket()         
        s.bind((host, port))       

        print("Listening on port: %s" % str(port))

        s.listen(5)                 
        c, addr = s.accept()        

        print("Received request from: " + str(addr))
        auth = OAuthHandler(consumer_key, consumer_secret)
        auth.secure = True
        auth.set_access_token(access_token, access_secret)

        api = API(auth)

        streamListener = TwitterStreamListener(c)
        myStream = tweepy.Stream(auth=api.auth, listener=streamListener, tweet_mode='extended')
        myStream.filter(track=['Trump'])






