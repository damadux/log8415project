import pymysql
import sshtunnel
import subprocess
import random
from sshtunnel import SSHTunnelForwarder

# Processes the requests
# requests: a list of sql requests to make on the sql database
# method: the method used in fowarding the requests (0: direct, 1: random, 2: customized) 
def processRequests(requests,method,master_ip,data_1_ip,data_2_ip,data_3_ip):

	
	# Tunnel for slave nodes
	tunnel_data1 = SSHTunnelForwarder(
		(data_1_ip, 22),
		ssh_username = 'ubuntu',
		ssh_pkey='vockey',
		ssh_config_file = None,
		remote_bind_address = (master_ip, 3306)
		)
		
	tunnel_data2 = SSHTunnelForwarder(
		(data_2_ip, 22),
		ssh_username = 'ubuntu',
		ssh_pkey='vockey',
		ssh_config_file = None,
		remote_bind_address = (master_ip, 3306)
		)
		
	tunnel_data3 = SSHTunnelForwarder(
		(data_3_ip, 22),
		ssh_username = 'ubuntu',
		ssh_pkey='vockey',
		ssh_config_file = None,
		remote_bind_address = (master_ip, 3306)
		)
		
	tunnel_data1.start()
	tunnel_data2.start()
	tunnel_data3.start()
	
	
	# Connect to master and slave nodes
	connection_master = pymysql.connect(
		host=master_ip,
		user='root',
		password='',
		db='sakila')
		
		
		
	
	connection_data1 = pymysql.connect(
		host=master_ip,
		user='root',
		password='',
		db='sakila',
		port=tunnel_data1.local_bind_port
	
	)
	connection_data2 = pymysql.connect(
		host=master_ip,
		user='root',
		password='',
		db='sakila',
		port=tunnel_data2.local_bind_port
	
	)
	connection_data3 = pymysql.connect(
		host=master_ip,
		user='root',
		password='',
		db='sakila',
		port=tunnel_data3.local_bind_port
	
	)
	
	# direct hit method
	if method == 0:
	
		with connection_master:
			with connection_master.cursor() as cursor:
				for request in requests:
					cursor.execute(request)
			connection_master.commit()
	# random method
	if method == 1:
		for request in requests:
			prov = random.randrange(4)
			if (prov == 0):
				# Master server
				with connection_master:
					with connection_master.cursor() as cursor:
						cursor.execute(request)
					connection_master.commit()
			if (prov == 1):
				with connection_data1:
					with connection_data1.cursor() as cursor:
						cursor.execute(request)
					connection_data1.commit()
			if (prov == 2):
				with connection_data2:
					with connection_data2.cursor() as cursor:
						cursor.execute(request)
					connection_data2.commit()
			if (prov == 3):
				with connection_data3:
					with connection_data3.cursor() as cursor:
						cursor.execute(request)
					connection_data3.commit()
	# ping method
	if method == 2:
	
		# ping each server to get the min amount of ping
		chosen_server = 0;
		
		output = subprocess.check_output(['ping', master_ip ,'-c 1'], capture_output=True)
		time_index = output.index("time=")
		master_ping = output.substring(time_index + 5, time_index + 10)
		
		chosen_ping = master_ping
		
		output = subprocess.check_output(['ping', data_1_ip ,'-c 1'], capture_output=True)
		time_index = output.index("time=")
		data_1_ping = output.substring(time_index + 5, time_index + 10)
		
		if (chosen_ping > data_1_ping):
			chosen_server = 1
			chosen_ping = data_1_ping
			
		output = subprocess.check_output(['ping', data_2_ip ,'-c 1'], capture_output=True)
		time_index = output.index("time=")
		data_2_ping = output.substring(time_index + 5, time_index + 10)
		
		if (chosen_ping > data_2_ping):
			chosen_server = 2
			chosen_ping = data_2_ping
			
		output = subprocess.check_output(['ping', data_3_ip ,'-c 1'], capture_output=True)
		time_index = output.index("time=")
		data_3_ping = output.substring(time_index + 5, time_index + 10)
		
		
		if (chosen_ping > data_3_ping):
			chosen_server = 3
			chosen_ping = data_3_ping
		
		# Now that min ping has been determined, run request on it
		if (prov == 0):
			# Master server
			with connection_master:
				with connection_master.cursor() as cursor:
					cursor.execute(request)
				connection_master.commit()
		if (prov == 1):
			with connection_data1:
				with connection_data1.cursor() as cursor:
					cursor.execute(request)
				connection_data1.commit()
		if (prov == 2):
			with connection_data2:
				with connection_data2.cursor() as cursor:
					cursor.execute(request)
				connection_data2.commit()
		if (prov == 3):
			with connection_data3:
				with connection_data3.cursor() as cursor:
					cursor.execute(request)
				connection_data3.commit()
		
		
		
	
		
