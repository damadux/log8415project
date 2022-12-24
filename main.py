

from proxy import processRequests
import sys
import boto3


if __name__ == '__main__':

	if (len(sys.argv) != 6)
		sys.exit("Must have 6 arguments: master node ip, data node 1 ip, data node 2 ip, data node 3 ip, sql requests file path, method used (0:master, 1:random, 2:ping)")
	
	sql_requests_file = str(sys.argv[4])
	#read file of sql requests, put each line in an array
	with open(sql_requests_file, "r") as sql_f:
		sql_requests = [line for line in sql_f]
	
	master_ip = str(sys.argv[0])
	data_1_ip = str(sys.argv[1])
	data_2_ip = str(sys.argv[2])
	data_3_ip = str(sys.argv[3])
	
	# process requests
	processRequests(sql_requests,str(sys.argv[5]),master_ip,data_1_ip,data_2_ip,data_3_ip)

