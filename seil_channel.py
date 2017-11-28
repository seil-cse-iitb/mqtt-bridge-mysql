import requests
import json
import mysql.connector
from mysql.connector import errorcode
from config import *
API_ROOT = "http://10.129.23.41:8080/meta/"


class SeilChannel:

	def __init_table(self):
		self.cnx = mysql.connector.connect(user=config['username'], password=config['password'], host=config['host'], database=config['database'])
		self.cursor = self.cnx.cursor()
		self.table_name = "%s_%d" % (self.sensor_type, self.id)
		sql_command = "CREATE TABLE IF NOT EXISTS %s_%d (" % (self.sensor_type, self.id)
		sql_command += " `sensor_id` VARCHAR(20) NOT NULL, "
		for field in self.fields:
			if field['field_type'] == 'F':
				sql_command += " `%s` double, " % (field['field_name'])
			if field['field_type'] == 'I':
				sql_command += " `%s` int(11), " % (field['field_name'])

		sql_command += "  PRIMARY KEY (`sensor_id`,`TS`) \
		) ENGINE=InnoDB"

		# CONSTRAINT `sensor_id_fk` FOREIGN KEY (`sensor_id`) \
		# REFERENCES `seil_rest_api`.`meta_sensor` (`sensor_id`)\

		try:
			print("Creating table {}: ")
			self.cursor.execute(sql_command)
		except mysql.connector.Error as err:
			if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
				print("already exists.")
			else:
				print(err.msg)
	"""
		Load channel and sensors meta data from rest API
	"""
	def __load_channel(self):

		# load channels
		url = API_ROOT + "channels/"
		try:
			response = requests.get(url)
			all_channels = json.loads(response.text)
			for channel in all_channels:
				if channel['id'] == self.id:
					self.sensor_type = channel['sensor_type']
					self.display_name = channel['display_name']
					self.channel_type = channel['channel']

			# Fetch field info for this channel
			url = API_ROOT + 'channel/%d' % self.id
			self.fields = json.loads(requests.get(url).text)

		except Exception as e:
			print(e.__traceback__)

		# Load sensors
		url = API_ROOT + "sensors/"
		try:
			response = requests.get(url)
			self.sensors = json.loads(response.text)
		except Exception as e:
			print(e.__traceback__)

	def __prepare_insert_query(self):
		self.insert_query = "INSERT INTO %s (sensor_id, " % (self.table_name)
		self.insert_query += ', '.join(f['field_name'] for f in self.fields) + ") VALUES (%s, "
		self.insert_query += ', '.join( "%s" for f in self.fields) + ")"
		return self.insert_query

	def __init__(self, channel_id):

		self.id = channel_id
		self.__load_channel()
		self.__init_table()
		self.__prepare_insert_query()
		self.inserts=[]

	def push_tuple(self, datapoints):
		if len(datapoints) != len(self.fields):
			print("Datapoint number mismatch")
			return False

		self.inserts.append(self.insert_query % datapoints)
		return True

	def commit(self):
		pass