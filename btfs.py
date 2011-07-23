#!/usr/bin/python
__credits__ = """Copyright (c) 2011 Roman Beslik <rabeslik@gmail.com>
Copyright (c) 2011 eurekafag <eurekafag@eureka7.ru>
Licensed under GNU LGPL 2.1 or later.  See <http://www.fsf.org/>.

This library is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License as published by the Free Software Foundation; either
version 2.1 of the License, or (at your option) any later version.

This library is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public
License along with this library; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
"""

# to do:
# libtorrent-rasterbar to python3
# "mplayer" does not respect "-cache" and "-cache-min" after seek
# comment the source code

# done:

import libtorrent
import threading
import os
import os.path as fs
import sys
import io
import getopt
import signal
import fuse
import errno
import stat
import time
from time import sleep

import logging
import logging.config
import json
import traceback

class reference(object):
	pass

def range_spec_len(r):
	return r.last+1-r.first

def coerce_piece_par(s):
	try:
		b = int(s)
	except ValueError:
		return (227, "\"--piece-par\" is not an integer")
	if b<=0:
		return (228, "\"--piece-par\" must be positive")
	else:
		return (0, b)

class piece_server(object):
	def __init__(self):
		super(piece_server, self).__init__()
		self.lock = threading.Lock()
		self.array = []
	def init(self):
		self.torrent_handle.prioritize_pieces(self.torrent_info.num_pieces() * [0])
	def push(self, read_piece_alert):
		self.lock.acquire()
		try:
			def f(record):
				_, _, piece = record
				return piece == read_piece_alert.piece
			for record in filter(f, self.array):
				event, channel, _ = record
				channel.append(read_piece_alert.buffer)
				event.set()
			self.array = filter(lambda record: not(f(record)), self.array)
		except:
			logger = logging.getLogger("root")
			logger.critical(str("".join(traceback.format_exception(*sys.exc_info()))))
		finally:
			self.lock.release()
	def pop(self, piece):
		logger = logging.getLogger("root")
		event = threading.Event()
		channel = []
		self.lock.acquire()
		try:
			p1 = min(self.torrent_info.num_pieces(), piece+self.piece_par.data)
			
			#deadline = 0
			#for pa in range(piece, p1):
			#	self.torrent_handle.set_piece_deadline(pa, deadline, 0)
			#	self.torrent_handle.piece_priority(pa, 1)
			#	deadline += 1000
			for pa in range(piece, p1):
				self.torrent_handle.piece_priority(pa, 1)
			
			logger.debug("downloading of the pieces ["+str(piece)+", "+str(p1)+"] is turned on")
			self.array.append((event, channel, piece))
			self.torrent_handle.set_piece_deadline(piece, 0, libtorrent.deadline_flags.alert_when_available)
		except:
			logger = logging.getLogger("root")
			logger.critical(str("".join(traceback.format_exception(*sys.exc_info()))))
		finally:
			self.lock.release()
		event.wait()
		return channel[0]

class alert_client(threading.Thread):
	def __init__(self):
		super(alert_client, self).__init__()
		self.daemon = True
	def run(self):
  	  try:
		logger = logging.getLogger("root")
		self.torrent_session.set_alert_mask(libtorrent.alert.category_t.storage_notification + libtorrent.alert.category_t.status_notification)
		while(True):
			self.torrent_session.wait_for_alert(1000)
			a = self.torrent_session.pop_alert()
			if (type(a) == libtorrent.read_piece_alert):
				logger.info("the piece "+str(a.piece)+" is received")
				self.piece_server.push(a)
			if (type(a) in [libtorrent.save_resume_data_alert, libtorrent.save_resume_data_failed_alert]
				and not (self.resume_alert is None)):
				self.resume_alert.push(a)
			if (type(a) == libtorrent.fastresume_rejected_alert):
				logger.info("resume data is rejected"
					# " because \""+str(a.error.message())+"\" (error code="+str(a.error.value())+")")
					)

	  except:
		  logger = logging.getLogger("root")
		  logger.critical(str("".join(traceback.format_exception(*sys.exc_info()))))
		  
class torrent_file_bt2p(object):
	def write(self, size, offset):
           try:
		logger = logging.getLogger("root")
		request_done = False
		last = offset + size - 1
		result = ""
		while (not(request_done)):
			piece_slice = self.map_file(offset)
			logger.info("the piece "+str(piece_slice.piece)+" is requested")
			data = self.piece_server.pop(piece_slice.piece)
			if data is None:
				logger.warning("pop_piece() is None for the piece "+str(piece_slice.piece))
				request_done = True
			else:
				available_length = last - offset + 1
				end = piece_slice.start + available_length
				if (end >= len(data)):
					end = len(data)
					available_length = len(data) - piece_slice.start
				logger.debug("writing the data=(piece="+str(piece_slice.piece) \
					+", interval=["+str(piece_slice.start)+", "+str(end)+"))")
				result += data[piece_slice.start:end]
				logger.debug("the data is written")
				offset += available_length
				request_done = offset > last
		return result
	   except:
		   logger = logging.getLogger("root")
		   logger.critical(str("".join(traceback.format_exception(*sys.exc_info()))))

class torrent_read_bt2p(object):
	def init(self):
		self.info_hash = self.torrent_info.info_hash()
	def find_file(self, path):
		for i, f0 in enumerate(self.torrent_info.files()):
			if f0.path == path:
				f1 = torrent_file_bt2p()
				f1.size = f0.size
				f1.map_file = lambda offset: self.torrent_info.map_file(i, offset, 1)
				f1.piece_server = self.piece_server
				return f1
		return None

class resume_alert(object):
	def __init__(self):
		super(resume_alert, self).__init__()
		self.lock = threading.Lock()
	def push(self, alert):
		self.lock.acquire()
		try:
			self.channel.append(alert)
			self.event.set()
		finally:
			self.lock.release()
	def pop(self):
		event = threading.Event()
		channel = []
		self.lock.acquire()
		try:
			self.channel = channel
			self.event = event
			self.torrent_handle.save_resume_data()
		finally:
			self.lock.release()
		event.wait()
		return channel[0]

class resume_save(object):
	def __init__(self):
		super(resume_save, self).__init__()
		self.lock = threading.Lock()
		self.last = False
	def save(self, last):
		self.lock.acquire()
		try:
			if not self.last:
				logger = logging.getLogger("root")
				alert = self.resume_alert.pop()
				if type(alert)==libtorrent.save_resume_data_alert:
					io.open(self.file_name, "wb").write(libtorrent.bencode(alert.resume_data))
					logger.debug("resume data is written to: "+self.file_name)
				else:
					logger.warning("can not obtain resume data, error code: "+str(alert.error_code))
				self.last = last
		finally:
			self.lock.release()

class resume_timer(threading.Thread):
	def __init__(self):
		super(resume_timer, self).__init__()
		self.daemon = True
	def run(self):
		while(True):
			sleep(60) # configuration. a time in seconds between savings of resume data
			self.resume_save.save(False)

def error_exit(exit_code, message):
	sys.stderr.write(message+"\n")
	sys.exit(exit_code)

class term_handler(object):
	def __init__(self):
		super(term_handler, self).__init__()
	def scavenge_pid(self):
		pass
	def save_resume(self):
		pass
	def do(self):
		if not (self.save_resume is None):
			self.save_resume()
		if not (self.scavenge_pid is None):
			self.scavenge_pid()
		sys.exit(os.EX_OK)
	def hdo(self, x, y):
		self.do()

fuse.fuse_python_api = (0, 2)

class MyStat(fuse.Stat):
	def __init__(self):
		self.st_mode = stat.S_IFDIR | 0755
		self.st_ino = 0
		self.st_dev = 0
		self.st_nlink = 2
		self.st_uid = os.geteuid()
		self.st_gid = os.getegid()
		self.st_size = 4096
		self.st_atime = time.time()
		self.st_mtime = time.time()
		self.st_ctime = time.time()

class BTFS(fuse.Fuse):
	def __init__(self, *args, **kw):
		self.logger = logging.getLogger("root")
		fuse.Fuse.__init__(self, *args, **kw)

	def fsinit(self):
               try:
		if "resume" in self.options:
			resume = self.options["resume"]
		else:
			resume = None
		resume_data = main_resume(resume)
	
		if "resume" in self.options:
			def f():
				self.logger.debug("shutting down with a resume")
				torrent_session.pause()
				rs.save(True)
				
		torrent_session = libtorrent.session()
		sp = libtorrent.session_settings()
		sp.request_timeout /= 10 # configuration
		sp.piece_timeout /= 10 # configuration
		sp.peer_timeout /= 20 # configuration
		sp.inactivity_timeout /= 20 # configuration
		torrent_session.set_settings(sp)
		torrent_session.listen_on(self.options["port"], self.options["port"] + 100)
	
		e = libtorrent.bdecode(io.open(self.options["hash-file"], 'rb').read())
		torrent_info = libtorrent.torrent_info(e)
		torrent_descr = {"storage_mode": libtorrent.storage_mode_t.storage_mode_allocate
				 , "save_path": self.options["save-path"]
				 , "ti": torrent_info}
		if not (resume_data is None):
			torrent_descr["resume_data"] = resume_data
		self.logger.debug("the torrent description: "+str(torrent_descr))
		torrent_handle = torrent_session.add_torrent(torrent_descr)
		self.logger.debug("the torrent handle "+str(torrent_handle)+" is created")
	
		piece_par_ref0 = reference()
		piece_par_ref0.data = self.options["piece-par"]
		
		piece_server0 = piece_server()
		piece_server0.torrent_handle = torrent_handle
		piece_server0.torrent_info = torrent_info
		piece_server0.piece_par = piece_par_ref0
		piece_server0.init()

		alert_client0 = alert_client()
		alert_client0.torrent_session = torrent_session
		alert_client0.piece_server = piece_server0
		if "resume" in self.options:
			ra = resume_alert()
			ra.torrent_handle = torrent_handle
			
			rs = resume_save()
			rs.file_name = self.options["resume"]
			rs.resume_alert = ra
		
			rt = resume_timer()
			rt.resume_save = rs
			rt.start()
		
			alert_client0.resume_alert = ra
		else:
			alert_client0.resume_alert = None
	
		alert_client0.start()
		r = torrent_read_bt2p()
		r.torrent_handle = torrent_handle
		r.torrent_info = torrent_info
		r.piece_server = piece_server0
		r.init()
		self.torrent_handle = torrent_handle
		self.torrent_info = torrent_info
		self.torrent = r
		self.piece_server = piece_server0
		self.parsebttree()
               except:
		     logging.critical(str("".join(traceback.format_exception(*sys.exc_info()))))
			
	def parsebttree(self):
		class btentry(dict):
			pass
		
		self.files = btentry() # struct: "filedirname1": { ... }, "filedirname1": true,  .mode: mode, .size: size
		for f in self.torrent_info.files():
			filename = f.path
			if f.path.startswith(self.torrent_info.name() + "/"):
				splitname = f.path.split("/", 1)
				if len(splitname) > 1:
					filename = splitname[1]

			splitfn = filename.split("/")
			self.logger.debug("Parsing: " + unicode(splitfn))
			partnum = 1
			curfile = self.files
			for pathpart in splitfn:
				if partnum == len(splitfn):
					curfile[pathpart] = btentry()
					curfile[pathpart].mode = stat.S_IFREG | 0444
					curfile[pathpart].size = f.size
				else:
					if not pathpart in curfile:
						curfile[pathpart] = btentry()
						curfile[pathpart].mode = stat.S_IFDIR | 0755
						curfile[pathpart].size = 4096
						
					curfile = curfile[pathpart]
				partnum += 1

	def getbtdirlist(self, path):
		dirlist = []
		plainlist = []
		if path == "/":
			return [{"name": self.torrent_info.name(), "size": 4096, "mode": stat.S_IFDIR | 0755}], [self.torrent_info.name()]
		else:
			splitpath = path[1:].split("/", 1)
			if len(splitpath) > 1:
				path = splitpath[1]
			else:
				if splitpath[0] == self.torrent_info.name():
					path = ""
			splitpath = path.split("/")
			self.logger.debug("Split: " + unicode(splitpath))
			curfile = self.files

			contents = not splitpath[-1]

			if contents:
				del splitpath[-1]
				
			for pathpart in splitpath:
				if pathpart in curfile:
					curfile = curfile[pathpart]
				else:
					return None

			if contents:
				for f in curfile.keys():
					if not f in plainlist:
						dirlist.append({"name": f, "size": curfile[f].size, "mode": curfile[f].mode})
						plainlist.append(f)
			else:
				dirlist.append({"name": pathpart, "size": curfile.size, "mode": curfile.mode})
				plainlist.append(pathpart)

		return dirlist, plainlist

	def getattr(self, path):
		try:
			st = MyStat()
			tailpath = path[1:].split("/", 2)
			self.logger.debug("Getattr:" + path + " " + str(tailpath))
			if len(tailpath) > 1:
				btfile = self.getbtdirlist(path)
				if not btfile:
					return -errno.ENOENT

				btfile = btfile[0]
				self.logger.debug("Getpathattr:" + unicode(btfile))
				st.st_mode = btfile[0]["mode"]
				st.st_nlink = 1
				st.st_size = btfile[0]["size"]
			return st
                except:
			logging.critical(str("".join(traceback.format_exception(*sys.exc_info()))))

	def readdir(self, path, offset):
		try:
			self.logger.debug("ReadDir")
			dirents = [ '.', '..' ]
			self.logger.debug("Path: " + path)
			if path != "/":
				path += "/"
			dirlist = self.getbtdirlist(path)
			
			dirents.extend(dirlist[1])
			self.logger.debug("Dirents: " + unicode(dirents))
			for r in dirents:
				yield fuse.Direntry(r)
                except:
			self.logger.critical(str("".join(traceback.format_exception(*sys.exc_info()))))


	def read_from_torrent(self, torrent_file, size, offset):
		file_size = torrent_file.size
		if offset > file_size:
			return 0
		if offset + size > file_size:
			size = file_size - offset
			
		return torrent_file.write(size, offset)
			
	def read(self, path, size, offset):
		try:
			torrent_file = self.torrent.find_file(path[1:])
			if not torrent_file:
				self.logger.debug("Read404_1: " + path[1:])
				split_torrent_name = path[1:].split("/", 1)
				if len(split_torrent_name) > 1:
					self.logger.debug("Read404_2: " + split_torrent_name[1])
					torrent_file = self.torrent.find_file(split_torrent_name[1])
					if not torrent_file:
						return -errno.ENOENT
					
			self.logger.debug("Read: " + unicode(torrent_file.size) + " " + unicode(offset) + "+" + unicode(size))
			data = self.read_from_torrent(torrent_file, size, offset)
			return data
                except:
			self.logger.critical(str("".join(traceback.format_exception(*sys.exc_info()))))

def main_resume(resume):
	success = None
	resume_data = None
	if not (resume is None):
		if fs.exists(resume):
			if fs.isfile(resume):
				resume_data = io.open(resume, "rb").read()
				success = True
			else:
				success = False
		else:
			success = True
	else:
		success = True
	if success:
		return resume_data
	else:
		error_exit(224, "\"--resume\" is not a regular file")

def main_log(l):
	log, log_conf = l
	if log:
		if log_conf is None:
			log_conf = "/etc/btfs/logging.conf" # configuration
		else:
			error_exit(229, "both \"--log\" and \"--log-conf\" are given, the logging configuration file name is ambiguous")
	if not (log_conf is None):
		logging.config.fileConfig(log_conf)
	else:
		logging.disable(logging.CRITICAL)

def main_default(options):
	if not ("port" in options):
		options["port"] = 6881 # configuration
	if not ("piece-par" in options):
		options["piece-par"] = 2**3 # configuration
	if not ("hash-file" in options):
		error_exit(225,  "\"--hash-file\" is mandatory")
	elif not ("save-path" in options):
		error_exit(226, "\"--save-path\" is mandatory")

def main_torrent_descr(options, th):
	logger = logging.getLogger("root")
	def f():
		logger.debug("shutting down without a resume")
	th.save_resume = f
	
	main_default(options)
	try:
		fs = BTFS(dash_s_do='setsingle')
		fs.parse(errex=1)
		fs.options = options
		fs.main()
		
	except KeyboardInterrupt:
		th.do()

def main_options(options, l, th):
	main_log(l)
	main_torrent_descr(options, th)

def expandpath(path):
	return os.path.abspath(os.path.expandvars(os.path.expanduser(path)))
	
def main(argv=None):
	th = term_handler()
	signal.signal(signal.SIGTERM, th.hdo)
	if argv is None:
		argv = sys.argv
	try:
		crude_options, args = getopt.getopt(argv[1:], "f:s:r:p:r:"
			, ["resume=", "piece-par=", "log", "log-conf="])
	except getopt.error, error:
		error_exit(221, "the option "+error.opt+" is incorrect because "+error.msg)
	options = {}
	log = False
	log_conf = None
	for o, a in crude_options:
		if "--resume"==o or "-r"==o:
			options["resume"] = expandpath(a)
		elif "-f"==o:
			options["hash-file"] = expandpath(a)
		elif "-s"==o:
			options["save-path"] = expandpath(a)
		elif "--piece-par"==o:
			tag, value = coerce_piece_par(a)
			if 0==tag:
				options["piece-par"] = value
			else:
				error_exit(tag, value)
		elif "--log"==o:
			log = True
		elif "--log-conf"==o:
			log_conf = a
		elif "-p"==o:
			options["port"] = int(a)
		else:
			error_exit(223, "an unknown option is given")
	sys.argv[1:] = args
	main_options(options, (log, log_conf), th)

if __name__ == "__main__":
	main()
