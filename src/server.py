#!/usr/bin/env python
"""
The server program for Chain Replication Project
"""
import os, errno
import sys
import time
from threading import Thread
from socket import AF_INET, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR, socket, error


playlist = {}

#####
crashAfterReceive = False
crashAfterSend = False


class MasterListener(Thread):
	def __init__(self, pid, port):
		global alives, heartbeat_thread
		Thread.__init__(self)
		self.pid = pid
		# self.num_servers = num_servers
		self.port = port
		self.buffer = ""
		# for i in range(self.num_servers):
		# 	if i != pid:
		# 		listeners[i] = ServerListener(pid, i)
		# 		listeners[i].start()
		# for i in range(self.num_servers): 
		# 	if (i != pid): 
		# 		clients[i] = ServerClient(pid, i) 
		# 		clients[i].start()
		# heartbeat_thread = Heartbeat(pid)
		# heartbeat_thread.setDaemon(True)
		# heartbeat_thread.start()
		self.socket = socket(AF_INET, SOCK_STREAM)
		self.socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
		self.socket.bind((ADDR, self.port))
		self.socket.listen(1)
		self.master_conn, self.master_addr = self.socket.accept()
		self.connected = True


	def run(self):
		global playlist, crashAfterSend, crashAfterReceive
		while self.connected:
			if '\n' in self.buffer and not blocked:
				(l, rest) = self.buffer.split("\n", 1)
				print "{:d} receives {} from master".format(self.pid, l)
				self.buffer = rest
				cmd = l.split()[0]
				if cmd == "get":
					_, msgs = l.split(None, 1)
					url = playlist.get(msgs.strip(), 'NONE')
					self.master_conn.send("resp {}\n".format(url))
				elif cmd == "crashAfterReceiving":
					crashAfterReceive = True
				elif cmd == "crashAfterSending":
					crashAfterSend = False
				elif cmd == "crash":
					exit()
					break
				elif cmd == "add":
					args = l.split()
					#ToDo: 
						#	issues an add/edit request to the server at the head of the chain
					pass
				elif cmd == "delete":
					#Todo: 
					# issues an delete request to the server at the head of the chain
					pass
				elif cmd == "snapshot":
					pass
					#Todo: 
					# master sends it to the head of the chain 
					# as a normal message to go through the protocol. 
					# Upon receipt, each replica dumps the content of its two logs
					# and the id of the replica that precede it and follow it
					# in the chain, then sends them to the master.
					# The format will be shown in test cases, which will be published later.
				else:
					print "Unknown command {}".format(l)
			else:
				try:
					data = self.master_conn.recv(1024)
					self.buffer += data
				except:
					self.kill()
					break


	def kill(self):
		try:
			self.connected = False
			self.master_conn.close()
			self.socket.close()
		except:
			pass			



def main(id, port):
	pass


if __name__ == '__main__':
	args = sys.argv
	if len(args) != 3:
		print "Need two arguments!"
		os._exit(0)
	try:
		main(int(args[1]), int(args[2]))
	except KeyboardInterrupt: 
		os._exit(0)