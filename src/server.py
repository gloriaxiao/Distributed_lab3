#!/usr/bin/env python
"""
The server program for Chain Replication Project
"""
import os, errno
import sys
import time
from threading import Thread
from socket import AF_INET, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR, socket, error


class MasterListener(Thread):
	def __init__(self, pid, num_servers, port):
		global alives, heartbeat_thread
		Thread.__init__(self)
		self.pid = pid
		self.num_servers = num_servers
		self.port = port
		self.buffer = ""
		for i in range(self.num_servers):
			if i != pid:
				listeners[i] = ServerListener(pid, i)
				listeners[i].start()
		for i in range(self.num_servers): 
			if (i != pid): 
				clients[i] = ServerClient(pid, i) 
				clients[i].start()
		heartbeat_thread = Heartbeat(pid)
		heartbeat_thread.setDaemon(True)
		heartbeat_thread.start()
		self.socket = socket(AF_INET, SOCK_STREAM)
		self.socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
		self.socket.bind((ADDR, self.port))
		self.socket.listen(1)
		self.master_conn, self.master_addr = self.socket.accept()
		self.connected = True




def main(pid, num_servers, port):
	pass


if __name__ == '__main__':
	args = sys.argv
	if len(args) != 4:
		print "Need three arguments!"
		os._exit(0)
	try:
		main(int(args[1]), int(args[2]), int(args[3]))
	except KeyboardInterrupt: 
		os._exit(0)