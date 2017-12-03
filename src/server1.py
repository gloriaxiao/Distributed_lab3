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
ADDR = 'localhost'

playlist = {}
alives = {} 

suffix_listeners = {}
suffix_clients = {}
prefix_clients = {}
prefix_listeners = {}
wait_to_start = []

pred_id = -1
succ_id = -1
self_pid = -1

heartbeat_thread = None
master_thread = None

crashAfterReceive = False
crashAfterSend = False


update_log = [] 
ack_log = []


class Heartbeat(Thread): 
	def __init__(self, pid): 
		Thread.__init__(self)
		self.pid = pid

	def run(self): 
		global alives, succ_id, pred_id, prefix_clients, wait_to_start
		while True: 
			new_alives = {} 
			now = time.time()
			replace_succ = False
			replace_pred = False
			for key in alives: 
				if now - alives[key] <= 0.2: 
					new_alives[key] = alives[key]
				else:
					replace_succ = (succ_id == key)
					replace_pred = (pred_id == key)
			alives = new_alives
			# replace the predecessor, successor
			if replace_pred:
				pred_id = -1
				for k in sorted(alives.keys()):
					if k > self.pid:
						pred_id = k
						break
				print str(self.pid) + " decided to replace predecessor to " + str(pred_id)
				if len(ack_log) != 0: 
					prefix_clients[pred_id].send('ack ' + ','.join(ack_log))
			if replace_succ:
				succ_id = -1
				# print alives
				for k in sorted(alives.keys(), reverse=True):
					# print "k: " + str(k) + " pid: " + str(self.pid) 
					if k < self.pid:
						succ_id = k
						break
				print str(self.pid) + " decided to replace successor to " + str(succ_id)
				if len(update_log) != 0: 
					print update_log
					info_propagate(update_log[-1][1:-1].split(":")[0])
			time.sleep(0.2)


def info_propagate(command):
	global suffix_clients, crashAfterSend, ack_log, succ_id, self_pid
	global update_log, pred_id, prefix_clients
	if succ_id != -1: # not the tail
		send_msg = command
		if command == 'add' or command == 'delete':
			send_msg += ' ' + ",".join(update_log)
		print "{:d} sends {} to its successor {:d}\n".format(self_pid, send_msg, succ_id)
		suffix_clients[succ_id].send(send_msg)
		if crashAfterSend:
			exit()


def update_local_history(logs):
	global update_log, playlist, succ_id, ack_log, prefix_clients, self_pid
	for info in logs:
		if not info:
			continue
		if info not in update_log:
			update_log.append(info)
			cmd, args = info[1:-1].split(':', 1)
			if cmd == 'add':
				name, url = args.split(':')
				playlist[name] = url
			elif cmd == 'delete':
				del playlist[args.strip()]
		if succ_id == -1 and info not in ack_log:
			# it's the tail, update the ack_log as well
			ack_log.append(info)
	if pred_id != -1:
		print "{:d} sends ack {} to its pred {:d}\n".format(self_pid, ','.join(ack_log), pred_id)
		prefix_clients[pred_id].send('ack ' + ','.join(ack_log))
		# if succ_id == -1 and crashAfterSend:
		# 	exit()


def update_ack_log(logs):
	global ack_log
	for info in logs:
		if not info:
			continue
		if info not in ack_log:
			ack_log.append(info)


class MasterListener(Thread):
	def __init__(self, pid, port):
		Thread.__init__(self)
		global suffix_listeners, suffix_clients, prefix_clients, prefix_listeners
		global wait_to_start
		self.pid = pid
		self.port = port
		self.buffer = ""
		for i in range(12): 
			if (i == pid):
				continue
			elif (i < pid):
				suffix_listeners[i] = ServerListener(pid, i)
				suffix_listeners[i].start()
			else:
				prefix_listeners[i] = ServerListener(pid, i)
				prefix_listeners[i].start()
		for i in range(12):
			if (i == pid):
				continue
			elif (i < pid):
				suffix_clients[i] = ServerClient(pid, i)
				suffix_clients[i].start()
			else:
				prefix_clients[i] = ServerClient(pid, i)
				wait_to_start.append(i)
		heartbeat_thread = Heartbeat(pid)
		heartbeat_thread.start()
		self.socket = socket(AF_INET, SOCK_STREAM)
		self.socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
		self.socket.bind((ADDR, self.port))
		self.socket.listen(1)
		self.master_conn, self.master_addr = self.socket.accept()
		self.connected = True

	def run(self):
		global playlist, crashAfterSend, crashAfterReceive
		global update_log, ack_log, alives
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
					update_local_history(["<add:" + songName + ":" + URL + ">"])
					info_propagate(cmd)
				elif cmd == "delete":
					if crashAfterReceive: 
						exit() 
					_, songName = l.split(' ', 1)
					update_local_history(["<delete:" + songName + ">"])
					info_propagate(cmd)

				elif cmd == "snapshot":
					if crashAfterReceive: 
						exit()
					log = '<' + ''.join(update_log) + '>' + '<' + ''.join(ack_log) + '>'
					self.master_conn.send("snapshot " + log + "\n")
					info_propagate(cmd)
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
		global suffix_clients, prefix_clients, wait_to_start
		global succ_id, pred_id, update_log, ack_log, playlist
		global finish_init
		while True: 
			if "\n" in self.buffer: 
				(l, rest) = self.buffer.split("\n", 1)
				self.buffer = rest
				cmd = l.split()[0]
				if (l == "heartbeat"):
					alives[self.target_pid] = time.time()
					if (self.target_pid > succ_id and self.target_pid < self.pid):
						succ_id = self.target_pid
					if (self.target_pid > self.pid and pred_id == -1):
						# New Head
						print "{:d} acknowledge {:d} as the new head\n".format(self.pid, self.target_pid)
						pred_id = self.target_pid
						msgs = [','.join(playlist.keys()), ','.join(playlist.values()), ','.join(update_log), ','.join(ack_log)]
						info_msg = "newHead " + ';'.join(msgs)
						print "{:d} sends {:d} {}".format(self.pid, pred_id, info_msg)
						prefix_clients[pred_id].send(info_msg)
					if self.target_pid in wait_to_start: 
						print "{:d} start a client for {:d}".format(self.pid, self.target_pid)
						wait_to_start.remove(self.target_pid)
						prefix_clients[self.target_pid].start()

				elif cmd == "newHead":
					_, args = l.split(' ', 1)
					print "{:d} gets newHead information from {:d}".format(self.pid, self.target_pid)
					plist_key_info, plist_value_info, update_log_info, ack_log_info = args.split(';')
					plist_keys = plist_key_info.split(',')
					plist_values = plist_value_info.split(',')
					update_logs = update_log_info.split(',')
					ack_logs = ack_log_info.split(',')
					if plist_keys and not playlist:
						for i in range(len(plist_keys)): 
							playlist[plist_keys[i]] = plist_values[i]
					for log in update_logs:
						if log and log not in update_log:
							update_log.append(log)
					for log in ack_logs:
						if log and log not in ack_log:
							ack_log.append(log)
				
				elif cmd == "add" or cmd == "delete":
					if crashAfterReceive: 
						exit()
					_, args = l.split(' ', 1)
					logs = args.split(',')
					print "{:d} receives log from {:d}".format(self.pid, self.target_pid)
					print logs
					update_local_history(logs)
					info_propagate(cmd)

				elif cmd == "snapshot":
					if crashAfterReceive: 
						exit()
					log = '<' + ''.join(update_log) + '>' + '<' + ''.join(ack_log) + '>'
					print str(self.pid) + " sent " + log 
					master_thread.master_conn.send("snapshot " + log + "\n")
					info_propagate(cmd)

				elif cmd == "ack": 
					print str(self.pid) + " received " + l 
					_, log = l.split(' ', 1)
					update_ack_log(log.split(','))
					if pred_id == -1: # reach the head
						master_thread.master_conn.send("ack commit\n")
					else: # back propagation
						prefix_clients[pred_id].send(l)
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
			self.send("heartbeat")
			time.sleep(0.05)

	def send(self, msg): 
		if not msg.endswith("\n"): 
			msg = msg + "\n"
		try:
			self.sock.send(msg)
		except:
			if self.sock:
	  			self.sock.close()
	  			self.sock = None
			try: 
				s = socket(AF_INET, SOCK_STREAM)
				s.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
				s.connect((ADDR, self.port))
				self.sock = s 
				self.sock.send(msg)
			except:
				time.sleep(0.05)


	def kill(self):
		try:
			self.sock.close()
		except:
			pass


def exit():
	global suffix_listeners, suffix_clients, prefix_clients, prefix_listeners
	print "exit is called"
	for i in suffix_listeners:
		suffix_listeners[i].kill()
	for i in prefix_listeners:
		prefix_listeners[i].kill()
	for i in suffix_clients:
		suffix_clients[i].kill()
	for i in prefix_clients:
		prefix_clients[i].kill()
	os._exit(0)


def main(pid, port):
	global master_thread, self_pid
	self_pid = pid
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