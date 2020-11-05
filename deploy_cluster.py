import sys, getopt

"""
./deploy.sh node1 10.0.4.2 13802 10.0.4.2:13800,10.0.4.3:13800,10.0.4.4:13800,10.0.4.5:13800
./deploy.sh node1 10.0.4.2 13802 10.0.4.2:13800 10.0.4.3:13800 10.0.4.4:13800 10.0.4.5:13800

./deploy.sh node2 10.10.0.3 13803 10.10.0.2:13800,10.10.0.3:13800,10.10.0.4:13800,10.10.0.5:13800

./deploy.sh node3 10.10.0.4 13804 10.10.0.2:13800,10.10.0.3:13800,10.10.0.4:13800,10.10.0.5:13800

./deploy.sh node4 10.10.0.5 13805 10.10.0.2:13800,10.10.0.3:13800,10.10.0.4:13800,10.10.0.5:13800

"""

if __name__ == '__main__':

	argv = sys.argv[1:]
	_help = 'deploy_cluster.py -n <num_nodes>'

	if len(argv) < 2:
		print(_help)
		exit(1)
	try:
		opts, args = getopt.getopt(argv, "n:")
	except getopt.GetoptError:
		print(_help)
		exit(1)

	for opt, arg in opts:
		if opt == '-n':
			num_nodes = int(arg)


	port = "13800"
	base = "10.0.4."

	cluster = []
	deployment = []

	for replica in range(2, num_nodes+2):
		addr = base + str(replica) + ":" + port
		cluster.append(addr)

	port = int(port)+1
	name = "node"
	for num, replica in enumerate(cluster):
		node_name  = name + str(num+1)
		port += 1
		addr = replica.split(':')[0]
		rest = ','.join(cluster)
		print('./deploy.sh', node_name, addr, port, rest)