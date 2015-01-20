# coding-assignment
DS repository on GitHub
#!/usr/bin/python

# Crawls twitter based on hashtags (via streaming method) and store it in a MySQL database named 'crawlhashtags'

import config
import logging
import MySQLdb
import sys

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream 

# Twitter API authentication variables
CONSUMER_KEY = config.twitter_account['consumer_key']
CONSUMER_SECRET = config.twitter_account['consumer_secret']
ACCESS_KEY = config.twitter_account['access_key']
ACCESS_SECRET = config.twitter_account['access_secret']
HASHTAGS = config.filters['hashtags']
# Log file variables
LOG_FILE = config.logging.crawlhashtags['file']
LOG_LEVEL = config.logging.crawlhashtags['level'].upper()
# Database file variables
DB_HOST = config.db['host']
DB_PORT = config.db['port']
DB_USER = config.db['user']
DB_PASSWD = config.db['passwd']
DB_DB = config.db['db']
DB_CHARSET = config.db['charset']
DB_USE_UNICODE = config.db['use_unicode']

# Setting loglevel
if (LOG_LEVEL == 'DEBUG'):
	logging.basicConfig(filename=LOG_FILE,level=logging.DEBUG)
elif (LOG_LEVEL == 'INFO'):
	logging.basicConfig(filename=LOG_FILE,level=logging.INFO)
elif (LOG_LEVEL == 'WARNING'):
	logging.basicConfig(filename=LOG_FILE,level=logging.WARNING)
elif (LOG_LEVEL == 'ERROR'):
	logging.basicConfig(filename=LOG_FILE,level=logging.ERROR)
elif (LOG_LEVEL == 'CRITICAL'):
	logging.basicConfig(filename=LOG_FILE,level=logging.CRITICAL)
else:
	logging.error('Unknown loglevel: \'' + LOG_LEVEL + '\', default loglevel \'DEBUG\' is used.')

# A TweetsListener that listens to tweets based on the hashtags and stores the information into a MySQL database
class TweetsListener(StreamListener):
	# Initialises the object
	def init_listener(self, db = None):
		if (db == None):
			logging.error('Unexpected error: no connection to the database.')
			sys.exit(1)
		else:
			# Set the database object and the cursor used for making SQL query
			self.db = db
			self.db_connection = self.db.cursor()
			logging.info('Start to crawl tweets with hashtags: ' + str(HASHTAGS) + '.')

	# Process tweets as the listener receives them
	def on_status(self, status):
		try:
			# Assigns each attribute of a tweet to the corresponding column in the table 'tweets' in the database
			author_id = status.author.id
			author_screenName = status.author.screen_name
			created = status.created_at
			status_id = status.id
			lang = status.lang
			text = status.text
			timestamp = status.timestamp_ms

			try:
				raw_hashtags = status.entities.get('hashtags')
				if (raw_hashtags == None):
					logging.warning('Failed to read hashtags of a status.')
				else:
					hashtags_array = []
					for hashtag in raw_hashtags:
						try:
							# Converts the value into a plain text with utf8 encoding
							hashtags_array.append(hashtag['text'].encode('utf8'))
						except KeyError:
							logging.error('Failed to get hashtag\'s string.')
					hashtags = ','.join(repr(e) for e in hashtags_array)
					# Insert a tweet into the database
					self.db_connection.execute("INSERT IGNORE INTO tweets VALUES(%s, %s, %s, %s, %s, %s, %s, %s)", (author_id, author_screenName, created, status_id, lang, text, timestamp, hashtags))
					self.db.commit()
			except AttributeError, MySQLdb.IntegrityError:
				logging.error('Failed to read attribute: \'entities\' from a status.')
		except AttributeError:
			logging.error('Failed to read attribute: \'text\' from a status.')
		return True # continue listening
	def on_error(self, status_code):
		logging.error('Failed to stream twitter. Error status code: ' + status_code + '.')
		return True # continue listening
	def on_timeout(self):
		logging.error('Twitter StreamingAPI timeout.')
		return True # continue listening

def start_main():
	try:
		logging.info('Connecting to database on ' + DB_HOST + ':' + str(DB_PORT))
		# Connects to the database
		db = MySQLdb.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, passwd=DB_PASSWD, db=DB_DB, charset=DB_CHARSET, use_unicode=DB_USE_UNICODE)
		logging.info('Connected to database on ' + DB_HOST + ':' + str(DB_PORT))
		# Constructs a new Twitter listener object for streaming
		listener = TweetsListener()
		# Initialise the listener
		listener.init_listener(db)
		# Constructs a new OAuth handler and set the credentials
		auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
		auth.set_access_token(ACCESS_KEY, ACCESS_SECRET)
		# Constructs a Stream object
		stream = Stream(auth, listener)
		# Starts to stream tweets with hashtags in HASHTAGS
		stream.filter(track=HASHTAGS)
	except MySQLdb.Error, e:
		logging.error('Error %d: %s' % (e.args[0],e.args[1]))
	finally:
		if db:
			logging.info('Disconnecting from database.')
			# Disconnects from the database
			db.close()
			logging.info('Disconnected from database.')

if __name__ == '__main__':
	# Keep streaming until the script is interrupted
	while (True):
		start_main()
		#!/usr/bin/python

# Extracts data from MySQL database (according to config.py, it is crawlhashtags) and output it into a text file

import config
import logging
import MySQLdb
import time
import sys

HASHTAGS_ORI = config.filters['hashtags']
HASHTAGS = []
# Removes the '#' symbol for MySQL query string
for hashtag in HASHTAGS_ORI:
	HASHTAGS.append(hashtag[1:len(hashtag)])
# Log file variables
LOG_FILE = config.logging['extract_data']['file']
LOG_LEVEL = config.logging['extract_data']['level'].upper()
# Database variables
DB_HOST = config.db['host']
DB_PORT = config.db['port']
DB_USER = config.db['user']
DB_PASSWD = config.db['passwd']
DB_DB = config.db['db']
DB_CHARSET = config.db['charset']
DB_USE_UNICODE = config.db['use_unicode']
# Output variables
OUTPUT_FILE = config.output['file']

# Setting loglevel
if (LOG_LEVEL == 'DEBUG'):
	logging.basicConfig(filename=LOG_FILE,level=logging.DEBUG)
elif (LOG_LEVEL == 'INFO'):
	logging.basicConfig(filename=LOG_FILE,level=logging.INFO)
elif (LOG_LEVEL == 'WARNING'):
	logging.basicConfig(filename=LOG_FILE,level=logging.WARNING)
elif (LOG_LEVEL == 'ERROR'):
	logging.basicConfig(filename=LOG_FILE,level=logging.ERROR)
elif (LOG_LEVEL == 'CRITICAL'):
	logging.basicConfig(filename=LOG_FILE,level=logging.CRITICAL)
else:
	logging.error('Unknown loglevel: \'' + LOG_LEVEL + '\', default loglevel \'DEBUG\' is used.')

# Extracts data from the database, processes it and writes onto a text file
def extract_data():
	db = None
	try:
		logging.info('Connecting to database on ' + DB_HOST + ':' + str(DB_PORT))
		# Connects to the database and gets the cursor for query
		db = MySQLdb.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, passwd=DB_PASSWD, db=DB_DB, charset=DB_CHARSET, use_unicode=DB_USE_UNICODE)
		logging.info('Connected to database on ' + DB_HOST + ':' + str(DB_PORT))
		db_connection = db.cursor()
		# Finds out existing times in hours
		db_connection.execute("SELECT DISTINCT(DATE_FORMAT(created, '%Y-%m-%d %H')) from tweets DATETIME ORDER BY created");
		datetime_set = db_connection.fetchall();
		clean_datetime_set = []
		# Appends the zeroth minute and zeroth second to the time so that it can be used for querying
		for datetime in datetime_set:
			clean_datetime_set.append(datetime[0] + ":00:00")

		# Opens a file for writing (the output)
		f = open(OUTPUT_FILE, 'w')
		# Writes a header to the output file
		header = "time,all"
		for hashtag in HASHTAGS:
			header = header + "," + hashtag
		f.write(header + '\n')

		# For each hour, count the number of tweets and the number of tweets with each hashtag in HASHTAGS
		for ind in range(0, len(clean_datetime_set)):
			output = []
			output.append(clean_datetime_set[ind])
			if (ind < len(clean_datetime_set) - 1):
				db_connection.execute("SELECT COUNT(*) FROM tweets WHERE created >= %s AND created < %s", (clean_datetime_set[ind], clean_datetime_set[ind + 1]))
				output.append(db_connection.fetchone()[0])
				for hashtag in HASHTAGS:
					cmp_str = '%' + hashtag + '%'
					db_connection.execute("SELECT COUNT(*) FROM tweets WHERE created >= %s AND created < %s AND hashtags LIKE %s", (clean_datetime_set[ind], clean_datetime_set[ind + 1], cmp_str))
					output.append(db_connection.fetchone()[0])
				for output_ind in range(0, len(output)):
					if (output_ind < len(output) - 1):
						f.write(str(output[output_ind]) + ',')
					else:
						f.write(str(output[output_ind]) + '\n')
			else:
				db_connection.execute("SELECT COUNT(*) FROM tweets WHERE created >= %s", (clean_datetime_set[ind],))
				output.append(db_connection.fetchone()[0])
				for hashtag in HASHTAGS:
					cmp_str = '%' + hashtag + '%'
					db_connection.execute("SELECT COUNT(*) FROM tweets WHERE created >= %s AND hashtags LIKE %s", (clean_datetime_set[ind], cmp_str))
					output.append(db_connection.fetchone()[0])
				for output_ind in range(0, len(output)):
					if (output_ind < len(output) - 1):
						f.write(str(output[output_ind]) + ',')
					else:
						f.write(str(output[output_ind]) + '\n')
	except MySQLdb.Error, e:
		logging.error('Error %d: %s' % (e.args[0],e.args[1]))
	finally:
		if db:
			logging.info('Disconnecting from database.')
			# Disconnects from the database
			db.close()
			logging.info('Disconnected from database.')

# Starts the script
if __name__ == '__main__':
	extract_data()
	-- Creates a database named 'crawlhashtags'
CREATE DATABASE crawlhashtags;
-- Use the database 'crawlhashtags'
USE crawlhashtags;
-- Creates a table tweets with the following schema
CREATE TABLE `tweets` (
	`author_id` BIGINT NOT NULL,
	`author_screenName` VARCHAR(15) NOT NULL, -- twitter's screenName limit is 15
	`created` DATETIME NOT NULL,
	`status_id` BIGINT NOT NULL,
	`lang` VARCHAR(3), -- ISO-639 (needs 3 characters, e.g. 'und' which represents 'undefined')
	`text` TEXT, -- tweet limit is 140 but we have extraterrestrial alien characters set which we do not care
	`timestamp` BIGINT NOT NULL,
	`hashtags` TEXT, -- tweet limit is 140 but we have extraterrestrial alien characters set which we do not care
	PRIMARY KEY (`status_id`),
	INDEX `created_index` (`created`), -- index for fast query response
	INDEX `lang_index` (`lang`) -- index for fast query response
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
