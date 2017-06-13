from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
import time
import sys

consumer_key = "dwdAGQG5HzOvTkarMjXsnIZ6g"
consumer_secret = "vFt7VvToHURirsDI9cMXakUsogRPjd1RmFJ3yjOmlfFt52MOrR"
access_token = "75966464-QcJj4ZpIhJTBOThN4v9xKCx43tmFFH4YurxvGo54P"
access_token_secret = "zNGMnWGFdzz4XN4FfbtgfwCx7Qe6Wo3wC6hKkXNHSznZ4"

auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

class FileWriter(object):
    def __init__(self, dprefix, records_per_file=100):
        self.fprefix = "file-"
        self.fs      = 1
        self.fext    = ".dat"
        self.fcount  = 0
        self.dprefix = dprefix
        self.records_per_file = records_per_file

        self.fname = self.getNextFileName()
        self.fp = open(self.fname, "w")

    def getNextFileName(self):
        fname = self.dprefix + "/" + self.fprefix + str(self.fs) + self.fext
        self.fs += 1
        return fname

    def write(self, text):
        if self.fcount >= self.records_per_file:
            self.fname = self.getNextFileName()
            self.fp.close()
            self.fp = open(self.fname, "w")
            self.fcount = 0
            print "Changing file to ", self.fname

        self.fp.write("%s\n" % (text.replace("\n", "").encode('utf8')))
        self.fp.flush()
        self.fcount += 1


class StdOutListener(StreamListener):
    def on_data(self, data):
        global fwriter
        try:
            tweet = json.loads(data)
            fwriter.write(tweet["text"])
        except KeyError:
            time.sleep(1)

        return True

    def on_error(self, status):
        print status

# Select output directory
dprefix = sys.argv[1]
print "Destination directory set to ", dprefix
fwriter = FileWriter(dprefix)

l = StdOutListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, l)

#This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby'
stream.filter(track=['trump', 'Trump', 'Donald'])
