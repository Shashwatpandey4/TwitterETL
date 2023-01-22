import tweepy
import pandas as pd
import json
from datetime import datetime
import s3fs


def run_twitter_etl():

    Access_Token = "720983775899824129-3iaP6Yc42mrAvxrdvD0EN3bCNd1vdVs"
    Access_Token_Secret = "1K9FnCLBnwervn5AOBBpmwh7HwTaqs18siuMlrAtqmbmM"
    API_Key = "RfPKK1ZFgiQwE7Z7PcgmTMnXk"
    API_Key_Secret = "HVxAhfqLRfCsD34FDwvMijIJO7dhVjaGNkQ0FZbmOKkDpC7Ykk"

    #Authentication
    auth = tweepy.OAuthHandler(API_Key,API_Key_Secret)
    auth.set_access_token(Access_Token,Access_Token_Secret)

    #initializing api object
    api = tweepy.API(auth)

    tweets = api.user_timeline(screen_name='@elonmusk',
                                count = 100,
                                include_rts = False,
                                tweet_mode = 'extended') 


    #print(tweets)

    tweet_list = []

    for tweet in tweets:
        text = tweet._json["full_text"]

        refined_tweet = {"user" : tweet.user.screen_name,
                        'text': text,
                        'favorite_count' : tweet.favorite_count,
                        'retweet_count':tweet.retweet_count,
                        'created_at':tweet.created_at}

        tweet_list.append(refined_tweet)



    DF = pd.DataFrame(tweet_list)

    DF.to_csv("s3://twitter-shashwat/elon-tweets.csv")