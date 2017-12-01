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

successor_sender = None 
predecessor_sender = None 
successor_listener = None 
predecessor_listener = None 

heartbeat_thread = None
master_thread = None

crashAfterReceive = False
crashAfterSend = False

updates_log = [] 
acknowledgements_log = []

class MasterListener(Thread):
	def __init__(self, pid, port):
		Thread.__init__(self)
		global successor_listener, successor_sender, server_listeners, predecessor_listener, predecessor_sender
		self.pid = pid
		self.port = port
		self.buffer = ""
		for i in range(10): 
			if (i != pid): 
				server_listeners[i] = ServerListener(pid, i)
				server_listeners[i].start()
		for i in range(pid - 1, -1, -1): 
			print str(pid) + " trying to connect to " + str(i)
			try: 
				successor_sender = ServerClient(pid, i)
				successor_sender.start() 
				successor_listener = server_listeners[i]
				break 
			except: 
				print str(pid) + " cannot connect to " + str(i)
		self.socket = socket(AF_INET, SOCK_STREAM)
		self.socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
		self.socket.bind((ADDR, self.port))
		self.socket.listen(1)
		self.master_conn, self.master_addr = self.socket.accept()
		self.connected = True


	def run(self):
		global playlist, crashAfterSend, crashAfterReceive
		global updates_log, acknowledgements_log
		while self.connected:
			if '\n' in self.buffer:
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
					crashAfterSend = True 
				elif cmd == "crash":
					exit()
				elif cmd == "add":
					if crashAfterReceive: 
						exit() 
					_, args = l.split(' ', 1)
					songName, URL = args.split(' ', 1)
					playlist[songName] = URL 
					updates_log.append("<add:" + songName + ":" + URL + ">")
					if successor_sender: # not the tail 
						successor_sender.send(l)
						if crashAfterSend: 
							exit() 
					else: # replica is both head and tail 
						acknowledgements_log.append("<delete:" + songName + ">")
						self.master_conn.send('ack commit\n')
				elif cmd == "delete":
					if crashAfterReceive: 
						exit() 
					_, songName = l.split(' ', 1)
					del playlist[songName]
					updates_log.append("<delete:" + songName + ">")
					if successor_sender: # not the tail 
						successor_sender.send(l) 
						if crashAfterSend: 
							exit() 
					else: # replica is both head and tail 
						acknowledgements_log.append("<delete:" + songName + ">")
						self.master_conn.send('ack commit\n')
				elif cmd == "snapshot":
					if crashAfterReceive: 
						exit() 
					log = '<' + ''.join(updates_log) + '>' + '<' + ''.join(acknowledgements_log) + '>'
					# print str(self.pid) + " sent " + log 
					self.master_conn.send("snapshot " + log + "\n")
					if successor_sender:
						successor_sender.send(l) 
						if crashAfterSend: 
							exit() 
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
		global predecessor_listener, predecessor_sender
		while True: 
			if "\n" in self.buffer: 
				(l, rest) = self.buffer.split("\n", 1)
				self.buffer = rest 
				cmd = l.split()[0]
				if (l == "heartbeat"): 
					if self.target_pid > self.pid and predecessor_listener == None: 
						predecessor_sender = ServerClient(self.pid, self.target_pid)
						predecessor_listener = server_listeners[self.target_pid]
						predecessor_sender.send("info " + ','.join(playlist.keys()) 
							+ ' ' + ','.join(playlist.values()) + ' ' + ','.join(updates_log) 
							+ ' ' + ','.join(acknowledgements_log))
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
				elif cmd == "add":
					if crashAfterReceive: 
						exit() 
					_, args = l.split(' ', 1)
					songName, URL = args.split(' ', 1)
					playlist[songName] = URL 
					print playlist 
					updates_log.append("<add:" + songName + ":" + URL + ">")
					if successor_sender: # not the tail 
						successor_sender.send(l)
						if crashAfterSend: 
							exit() 
					else: # replica is both head and tail 
						acknowledgements_log.append("<add:" + songName + ":" + URL + ">")
						predecessor_sender.send("ack " + l)
				elif cmd == "delete":
					if crashAfterReceive: 
						exit() 
					_, songName = l.split(' ', 1)
					del playlist[songName]
					updates_log.append("<delete:" + songName + ">")
					if successor_sender: # not the tail 
						successor_sender.send(l) 
						if crashAfterSend: 
							exit() 
					else: # replica is both head and tail 
						acknowledgements_log.append("<delete:" + songName + ">")
						predecessor_sender.send("ack " + l)
				elif cmd == "snapshot":
					if crashAfterReceive: 
						exit() 
					log = '<' + ''.join(updates_log) + '>' + '<' + ''.join(acknowledgements_log) + '>'
					# print str(self.pid) + " sent " + log 
					master_thread.master_conn.send("snapshot " + log + "\n")
					if successor_sender:
						successor_sender.send(l) 
						if crashAfterSend: 
							exit() 
				elif cmd == "ack": 
					print str(self.pid) + " received ack " + l 
					_, args = l.split(' ', 1)
					op, song_info = args.split(' ', 1)
					if op == "add": 
						songName, URL = song_info.split(' ', 1)
						acknowledgements_log.append("<add:" + songName + ":" + URL + ">")
					elif op == "delete": 
						acknowledgements_log.append("<delete:" + song_info + ">")
					else: 
						print "Unknown command {}".format(l)
					if predecessor_sender == None: 
						master_thread.master_conn.send("ack commit\n")
					else: 
						predecessor_sender.send(l)
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
		s = socket(AF_INET, SOCK_STREAM)
		s.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
		s.connect((ADDR, self.port))
		self.sock = s 
		self.sock.send("heartbeat\n")

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
					# TODO: lost connection, need to reconnect to next one in chain 
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
	for i in server_listeners:
		server_listeners[i].kill()
	predecessor_sender.kill()
	successor_sender.kill()
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