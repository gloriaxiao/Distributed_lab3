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
ADDR = 'localhost'

playlist = {}
alives = {}
server_listeners = {} 
server_clients = {} 

heartbeat_thread = None
master_thread = None
#####
crashAfterReceive = False
crashAfterSend = False
blocked = False 

isHead = False 
isTail = False 
predecessor = -1 
successor = -1 

updates_log = [] 
acknowledgements_log = []

class Heartbeat(Thread): 
	def __init__(self, pid): 
		Thread.__init__(self)
		self.pid = pid

	def run(self): 
		global alives, isHead, isTail, predecessor, successor, updates_log, acknowledgements_log, playlist
		while True: 
			predecessor_crashed = False 
			successor_crashed = False 
			new_alives = {} 
			now = time.time()
			for key in alives: 
				if now - alives[key] <= 0.2: 
					new_alives[key] = alives[key]
			alives = new_alives
			if predecessor not in alives and predecessor != -1: 
				predecessor_crashed = True 
			if successor not in alives and successor != -1: 
				successor_crashed = True 
			if len(alives) == 0: 
				isHead = True 
				isTail = True 
				predecessor = -1 
				successor = -1 
			elif self.pid > max(alives.keys()): 
				isHead = True 
				isTail = False 
				successor = max(alives.keys()) 
				predecessor = -1 
			elif self.pid < min(alives.keys()): 
				isHead = False
				isTail = True 
				predecessor = min(alives.keys())
				successor = -1 
			else: 
				isHead = False
				isTail = False
				direct_bigger = 100
				direct_smaller = -1
				for i in alives.keys(): 
					if i > self.pid and i < direct_bigger: 
						direct_bigger = i 
					elif i < self.pid and i > direct_smaller: 
						direct_smaller = i 
				predecessor = direct_bigger
				successor = direct_smaller
			if predecessor_crashed and predecessor != -1 and len(acknowledgements_log) != 0: 
				print str(self.pid) + " " + str(alives)
				print str(self.pid) + " sent " + "crashACK " + acknowledgements_log[-1] + " to " + str(predecessor)
				# server_clients[predecessor].send("crashACK " + acknowledgements_log[-1])
				server_clients[predecessor].send('ack ' + ' '.join(acknowledgements_log[-1][1:-1].split(":")))
			if successor_crashed and successor != -1 and len(updates_log) != 0: 
				print str(self.pid) + " sent " + "crashCMD " + updates_log[-1] + " to " + str(successor)				
				# server_clients[successor].send("crashCMD " + updates_log[-1])
				server_clients[successor].send(' '.join(updates_log[-1][1:-1].split(":")))
			time.sleep(0.2)


class MasterListener(Thread):
	def __init__(self, pid, port):
		global heartbeat_thread
		Thread.__init__(self)
		self.pid = pid
		self.port = port
		self.buffer = ""
		for i in range(5): 
			if (i != pid): 
				server_listeners[i] = ServerListener(pid, i)
				server_listeners[i].start()
		for i in range(5): 
			if (i != pid): 
				server_clients[i] = ServerClient(pid, i) 
				server_clients[i].start()
		heartbeat_thread = Heartbeat(pid)
		heartbeat_thread.setDaemon(True)
		heartbeat_thread.start()
		self.socket = socket(AF_INET, SOCK_STREAM)
		self.socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
		# print 'pid to master ' + str(pid) + " binding to " + str(self.port)
		self.socket.bind((ADDR, self.port))
		self.socket.listen(1)
		self.master_conn, self.master_addr = self.socket.accept()
		self.connected = True


	def run(self):
		global playlist, crashAfterSend, crashAfterReceive, blocked
		global updates_log, acknowledgements_log
		while self.connected:
			if '\n' in self.buffer and not blocked:
				(l, rest) = self.buffer.split("\n", 1)
				# print "{:d} receives {} from master".format(self.pid, l)
				self.buffer = rest
				cmd = l.split()[0]
				if cmd == "get":
					if not isTail: 
						print "I, " + str(self.pid) + ", AM NOT THE TAIL"
					_, msgs = l.split(None, 1)
					url = playlist.get(msgs.strip(), 'NONE')
					self.master_conn.send("resp {}\n".format(url))
				elif cmd == "crashAfterReceiving":
					print str(self.pid) + " received crashAfterReceiving"
					crashAfterReceive = True
				elif cmd == "crashAfterSending":
					crashAfterSend = True 
				elif cmd == "crash":
					exit()
				elif cmd == "add":
					if not isHead: 
						print "I, " + str(self.pid) + ", AM NOT THE HEAD"
					if crashAfterReceive: 
						print str(self.pid) + " receiving next message, need crash"
						exit() 
					_, args = l.split(' ', 1)
					songName, URL = args.split(' ', 1)
					playlist[songName] = URL 
					updates_log.append("<add:" + songName + ":" + URL + ">")
					if successor != -1: 
						server_clients[successor].send(l) 
					else: 
						acknowledgements_log.append("<delete:" + songName + ">")
						self.master_conn.send('ack commit\n')
					if crashAfterSend: 
						exit() 
				elif cmd == "delete":
					if not isHead: 
						print "I, " + str(self.pid) + ", AM NOT THE HEAD"
					if crashAfterReceive: 
						exit() 
					_, songName = l.split(' ', 1)
					del playlist[songName]
					updates_log.append("<delete:" + songName + ">")
					if successor != -1: 
						server_clients[successor].send(l) 
					else: 
						acknowledgements_log.append("<delete:" + songName + ">")
						self.master_conn.send('ack commit\n')
					if crashAfterSend: 
						exit() 
				elif cmd == "snapshot":
					if not isHead: 
						print "I, " + str(self.pid) + ", AM NOT THE HEAD"
					log = '<' + ''.join(updates_log) + '>' + '<' + ''.join(acknowledgements_log) + '>'
					self.master_conn.send("snapshot " + log + "\n")
					if successor != -1:
						server_clients[successor].send(l) 
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
	def __init__(self, pid, target_pid): 
		Thread.__init__(self)
		self.pid = pid
		self.target_pid = target_pid 
		self.sock = socket(AF_INET, SOCK_STREAM)
		self.sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
		self.port = 29999 - pid * 100 - target_pid 
		# print 'pid ' + str(pid) + " binding to " + str(self.port)
		self.sock.bind((ADDR, self.port))
		self.sock.listen(1)
		self.buffer = ''

	def run(self): 
		self.conn, self.addr = self.sock.accept()
		global alives, master_thread, server_clients, updates_log, acknowledgements_log, playlist 
		while True: 
			if "\n" in self.buffer: 
				(l, rest) = self.buffer.split("\n", 1)
				self.buffer = rest 
				cmd = l.split()[0]
				# if (l != "heartbeat"): 
				# 	print "{:d} receives {} from {}".format(self.pid, l, self.target_pid)
				if (l == "heartbeat"): 
					if self.target_pid not in alives and isHead and self.target_pid > self.pid: 
						server_clients[self.target_pid].send("info " + ','.join(playlist.keys()) 
							+ ' ' + ','.join(playlist.values()) + ' ' + ','.join(updates_log) 
							+ ' ' + ','.join(acknowledgements_log))
					alives[self.target_pid] = time.time()
				elif cmd == "add":
					_, args = l.split(' ', 1)
					songName, URL = args.split(' ', 1)
					new_op = "<add:" + songName + ":" + URL + ">"
					if len(updates_log) == 0 or updates_log[-1] != new_op: 
						if crashAfterReceive: 
							print str(self.pid) + " receiving next message, need crash"
							exit() 
						playlist[songName] = URL 
						updates_log.append(new_op)
						if successor != -1: 
							server_clients[successor].send(l) 
						else: 
							acknowledgements_log.append("<add:" + songName + ":" + URL + ">")
							server_clients[predecessor].send("ack " + l) 
						if crashAfterSend: 
							exit() 
				elif cmd == "delete":
					_, songName = l.split(' ', 1)
					new_op = "<delete:" + songName + ">"
					if len(updates_log) == 0 or updates_log[-1] != new_op: 
						if crashAfterReceive: 
							print str(self.pid) + " receiving next message, need crash"
							exit() 
						print "before deleting " + str(playlist)
						del playlist[songName]
						updates_log.append(new_op)
						if successor != -1: 
							server_clients[successor].send(l) 
						else: 
							acknowledgements_log.append("<delete:" + songName + ">")
							server_clients[predecessor].send("ack " + l) 
						if crashAfterSend: 
							exit() 
				elif cmd == "snapshot":
					log = '<' + ''.join(updates_log) + '>' + '<' + ''.join(acknowledgements_log) + '>'
					master_thread.master_conn.send("snapshot " + log + "\n")
					if successor != -1: 
						server_clients[successor].send(l) 
				elif cmd == "ack": 
					_, args = l.split(' ', 1)
					op, song_info = args.split(' ', 1)
					if op == "add": 
						songName, URL = song_info.split(' ', 1)
						acknowledgements_log.append("<add:" + songName + ":" + URL + ">")
					elif op == "delete": 
						acknowledgements_log.append("<delete:" + song_info + ">")
					else: 
						print "Unknown command {}".format(l)
					if predecessor == -1: 
						master_thread.master_conn.send("ack commit\n")
					else: 
						server_clients[predecessor].send(l)
				elif cmd == "info": 
					_, args = l.split(' ', 1)
					playlist_key_info, other = args.split(' ', 1)
					playlist_value_info, other = other.split(' ', 1)
					updates_log_info, acknowledgements_log_info = other.split(' ', 1)
					if playlist_key_info != '' and len(playlist) == 0: 
						playlist_keys = playlist_key_info.split(',')
						playlist_values = playlist_value_info.split(',')
						for i in range(len(playlist_keys)): 
							playlist[playlist_keys[i]] = playlist_values[i]
					if updates_log_info != '' and len(updates_log) == 0: 
						updates_log.extend(updates_log_info.split(','))
					if acknowledgements_log_info != '' and len(acknowledgements_log) == 0: 
						acknowledgements_log.extend(acknowledgements_log_info.split(','))
					# print str(self.pid) + " " + str(playlist) + " " + str(updates_log) + " " + str(acknowledgements_log)
				# elif cmd == "crashACK": 
				# 	_, arg = l.split(' ', 1)
				# 	if acknowledgements_log[-1] != arg: 
				# 		acknowledgements_log.append(arg)
				# 		if predecessor == -1: 
				# 			master_thread.master_conn.send("ack commit\n")
				# 		else: 
				# 			server_clients[predecessor].send("ack " + ' '.join(arg[1:-1].split(':')))
				# elif cmd == "crashCMD": 
				# 	_, arg = l.split(' ', 1)
				# 	if updates_log[-1] != arg: 
				# 		words = arg.split(':')
				# 		if words[0] == 'add': 
				# 			playlist[words[1]] = words[2]
				# 		else: 
				# 			del playlist[words[1]]
				# 		updates_log.append(arg) 
				# 		if successor != -1: 
				# 			server_clients[successor].send(' '.join(arg[1:-1].split(':'))) 
				# 		else: 
				# 			acknowledgements_log.append(arg)
				# 			server_clients[predecessor].send("ack " + ' '.join(arg[1:-1].split(':'))) 
				else: 
					print "Unknown command {}".format(l)
			else: 
				try: 
					data = self.conn.recv(1024)
					if data == "": 
						raise ValueError
					self.buffer += data 
				except: 
					self.conn.close() 
					self.conn = None 
					self.conn, self.addr = self.sock.accept()	

	def kill(self):
		try:
			self.conn.close()
		except:
			pass


class ServerClient(Thread):
	def __init__(self, pid, target_pid):
		Thread.__init__(self)
		self.pid = pid
		self.target_pid = target_pid 
		self.port = 29999 - target_pid * 100 - pid
		self.sock = None 

	def run(self):
		while True: 
			try: 
				self.sock.send("heartbeat\n")
			except: 
				try: 
					self.sock = None 
					s = socket(AF_INET, SOCK_STREAM)
					s.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
					s.connect((ADDR, self.port))
					self.sock = s 
					self.sock.send("heartbeat\n")
				except: 
					pass 
			time.sleep(0.05) 

	def send(self, msg): 
		if not msg.endswith("\n"): 
			msg = msg + "\n"
		try: 
			self.sock.send(msg)
		except: 
			try: 
				self.sock = None 
				s = socket(AF_INET, SOCK_STREAM)
				s.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
				s.connect((ADDR, self.port))
				self.sock = s 
				self.sock.send(msg)
			except: 
				pass 

	def kill(self):
		try:
			self.sock.close()
		except:
			pass

def exit():
	print "exit is called"
	global server_clients, server_listeners
	for i in server_listeners:
		server_listeners[i].kill()
	for i in server_clients:
		server_clients[i].kill()
	os._exit(0)

def main(pid, port):
	global master_thread
	master_thread = MasterListener(pid, port)
	master_thread.start()

if __name__ == '__main__':
	args = sys.argv
	if len(args) != 3:
		print "Need two arguments!"
		os._exit(0)
	try:
		main(int(args[1]), int(args[2]))
	except KeyboardInterrupt: 
		os._exit(0)