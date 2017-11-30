#!/usr/bin/env python
"""
The server program for Chain Replication Project
"""
import os, errno
import sys
import time
from threading import Thread
from socket import AF_INET, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR, socket, error

###### MACROS ##########
SLEEP = 0.05
TIMEOUT = 0.2
BASEPORT = 20000


playlist = {}
alives = {}

heartbeat_thread = None
master_thread = None
#####
crashAfterReceive = False
crashAfterSend = False


class Heartbeat(Thread): 
	def __init__(self, pid): 
		Thread.__init__(self)
		self.pid = pid

	def run(self): 
		global alives
		while True: 
			new_alives = {} 
			now = time.time()
			for key in alives: 
				if now - alives[key] <= 0.2: 
					new_alives[key] = alives[key]
			alives = new_alives
			time.sleep(0.2)


class MasterListener(Thread):
	def __init__(self, pid, port):
		global heartbeat_thread
		Thread.__init__(self)
		self.pid = pid
		self.port = port
		self.buffer = ""
		heartbeat_thread = Heartbeat(pid)
		heartbeat_thread.setDaemon(True)
		heartbeat_thread.start()
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


class ServerListener(Thread): 
	def __init__(self, pid):
		global BASEPORT
		Thread.__init__(self)
		self.pid = pid
		self.sock = socket(AF_INET, SOCK_STREAM)
		self.sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
		self.port = BASEPORT + pid * 2
		self.sock.bind(('localhost', self.port))
		self.sock.listen(5)
		self.buffer = ''

	def run(self):
		pass


class ServerClient(Thread):

	def __init__(self, pid):
		pass

	def run(self):
		pass






def main(pid, port):
	global alives, master_thread
	master_thread = MasterListener(pid, port)
	for i in range(pid):



if __name__ == '__main__':
	args = sys.argv
	if len(args) != 3:
		print "Need two arguments!"
		os._exit(0)
	try:
		main(int(args[1]), int(args[2]))
	except KeyboardInterrupt: 
		os._exit(0)