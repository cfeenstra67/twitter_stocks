#!./twitterenv/bin/python3
from functools import wraps
from datetime import datetime
from time import sleep
from threading import Thread
import os

import json
with open(os.path.expanduser('~/Python_Projects/twitter_mining/credentials.json')) as f: credentials = json.load(f)

import tweepy as tpy
auth = tpy.OAuthHandler(credentials['consumer_key'], credentials['consumer_key_secret'])
auth.set_access_token(credentials['access_token'], credentials['access_token_secret'])
api = tpy.API(auth)

def listgen(some_gen):
	@wraps(some_gen)
	def wrapped(*args, **kwargs):
		return [*some_gen(*args, **kwargs)]
	return wrapped

def stock_symbols(filename='tickers.txt'):
	with open(filename, 'r') as f:
		return ['$' + val for val in f.read().split('\n')]

class StockListener(tpy.StreamListener):
	def __init__(self, max_time=None, max_tweets=None, max_filesize=None, max_responses=None,
		error_log='error.log', status_log='status.log', data_file='data.json'):
		super().__init__()
		self.start = datetime.now()
		self.count = 0
		self.response_count = 0
		self.max_time = max_time
		self.max_tweets = max_tweets
		self.max_filesize = max_filesize
		self.max_responses = max_responses
		self.error_log = error_log
		self.status_log = status_log
		self.data_file = data_file

	def on_data(self, data):
		self.count += 1
		self.response_count += 1
		with open(self.data_file, 'a') as f: f.write(data)
		return not self._should_terminate()

	def on_status(self, status):
		self.response_count += 1
		self.log_to(self.status_log, status)
		return not self._should_terminate()

	def on_error(self, error):
		self.response_count += 1
		self.log_to(self.error_log, error)
		if error == 420:
			t = Thread(target=start_listening, args=(1000,))
			t.start()
			return False
		return not self._should_terminate()

	def log_to(self, filename, code):
		with open(filename, 'a') as f: f.write('Value: %s Date: %s' % (*map(str, [code, datetime.now()]),))

	def _should_terminate(self):
		if self.max_time and (datetime.now() - self.start).total_seconds() >= self.max_time: return True
		if self.max_tweets and self.count >= self.max_tweets: return True
		if self.max_filesize and os.path.getsize(self.data_file) >= self.max_filesize: return True
		if self.max_responses and self.response_count >= self.max_responses: return True
		return False

def start_listening(delay=0, attributes={'max_filesize': 25 * (1024 ** 3)}):
	sleep(delay)
	stream = tpy.Stream(auth, StockListener(**attributes))
	stream.filter(track=stock_symbols())

pid_file = '.pid_python'

import atexit
def exit_handler():
	try:
		os.remove(pid_file)
	except: pass
	print('Completed at %s.' % str(datetime.now()))
atexit.register(exit_handler)

if __name__ == '__main__':
	import argparse
	parser = argparse.ArgumentParser()
	parser.add_argument('action', nargs=None, default=None, help='Perform action--"start", "stop", or "test"')
	args = parser.parse_args()
	if args.action:
		if args.action == 'start':
			if os.path.exists(pid_file): raise Exception('Process already running.')
			with open(pid_file, 'w+') as f: f.write(str(os.getpid()))
			start_listening()
		elif args.action == 'stop':
			try:
				with open(pid_file) as f: os.kill(int(f.read()), 1)
			except FileNotFoundError:
				raise Exception('No process currently running.')
		elif args.action == 'test':
			start_listening(attributes={'max_responses': 1})
