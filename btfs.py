#!/usr/bin/python
__credits__ = """Copyright (c) 2011 Roman Beslik <rabeslik@gmail.com>
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

import BaseHTTPServer
from SocketServer import ThreadingMixIn
import httpheader
from cgi import escape as escape_html
import urllib
from mimetypes import guess_type as guess_mime_type
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
			
			logger.debug("downloading of the pieces ["+str(piece)+", "+str(p1)+") is turned on")
			self.array.append((event, channel, piece))
			self.torrent_handle.set_piece_deadline(piece, 0, libtorrent.deadline_flags.alert_when_available)
		finally:
			self.lock.release()
		event.wait()
		return channel[0]

class alert_client(threading.Thread):
	def __init__(self):
		super(alert_client, self).__init__()
		self.daemon = True
	def run(self):
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

class torrent_file_bt2p(object):
	def write(self, dest, r):
		logger = logging.getLogger("root")
		request_done = False
		while (not(request_done)):
			piece_slice = self.map_file(r.first)
			logger.info("the piece "+str(piece_slice.piece)+" is requested")
			data = self.piece_server.pop(piece_slice.piece)
			if data is None:
				logger.warning("pop_piece() is None for the piece "+str(piece_slice.piece))
				request_done = True
			else:
				available_length = range_spec_len(r)
				end = piece_slice.start + available_length
				if (end>=len(data)):
					end = len(data)
					available_length = len(data)-piece_slice.start
				logger.debug("writing the data=(piece="+str(piece_slice.piece) \
					+", interval=["+str(piece_slice.start)+", "+str(end)+"))")
				dest.write(data[piece_slice.start:end])
				logger.debug("the data is written")
				r.first += available_length
				request_done = r.first>r.last

class torrent_read_bt2p(object):
	def write_html(self, s):
		return """<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
<title>"""+escape_html(self.torrent_info.name())+"""</title>
</head>
<body>
"""+s+"""
</body>
</html>
"""
	def write_html_index(self):
		s = ""
		for f in self.torrent_info.files():
			s += """
<tr>
	<td><a href="""+"\""+urllib.pathname2url("/"+f.path)+"\""+""">"""+escape_html(f.path)+"""</a></td>
	<td>"""+str(f.size)+"""</td>
</tr>
"""
		return self.write_html("""<p><strong>name:</strong> """+escape_html(self.torrent_info.name())+"""</p>
<p><strong>comment:</strong></p>
<p>"""+escape_html(self.torrent_info.comment())+"""</p>
<p><strong>size:</strong> """+str(self.torrent_info.total_size())+"""</p>
<p><strong>piece length:</strong> """+str(self.torrent_info.piece_length())+"""</p>
<table>
<tr>
	<th>file</th>
	<th>size</th>
</tr>
"""+s+"""</table>""")

	def init(self):
		self.info_hash = self.torrent_info.info_hash()
		self.html_index = self.write_html_index()
	def find_file(self, http_path):
		p = urllib.url2pathname(http_path)
		for i, f0 in enumerate(self.torrent_info.files()):
			if "/"+f0.path == p:
				f1 = torrent_file_bt2p()
				f1.size = f0.size
				f1.map_file = lambda offset: self.torrent_info.map_file(i, offset, 1)
				f1.content_type = guess_mime_type(f0.path)
				f1.piece_server = self.piece_server
				return f1
		return None

class http_responder_bt2p(BaseHTTPServer.BaseHTTPRequestHandler):
	def send_common_header(self):
		self.send_header("Accept-Ranges", "bytes")
		self.send_header("ETag", self.server.torrent.info_hash)
	def read_from_torrent(self, torrent_file):
		file_size = torrent_file.size
		def content_range_header(r):
			return "bytes "+str(r.first)+"-"+str(r.last)+"/"+str(file_size)
		content_type, content_encoding = torrent_file.content_type
		if content_type is None:
			content_type = "application/octet-stream"
			content_encoding = None
		def send_header_content_type():
			self.send_header("Content-Type", content_type)
			if not (content_encoding is None):
				self.send_header("Content-Encoding", content_encoding)
		def write_content_type():
			self.wfile.write("Content-Type: "+content_type+"\r\n")
			if not (content_encoding is None):
				self.send_header("Content-Encoding: "+content_encoding+"\r\n")
		hr = self.headers.get("Range")
		is_whole = True
		if not (hr is None): # http://deron.meranda.us/python/httpheader/
			try:
				hr_parsed = httpheader.parse_range_header(hr)
				try:
					hr_parsed.fix_to_size(file_size)
					hr_parsed.coalesce()
					self.send_response(206)
					self.send_common_header()
					if hr_parsed.is_single_range():
						r = hr_parsed.range_specs[0]
						send_header_content_type()
						self.send_header("Content-Length", range_spec_len(r))
						self.send_header("Content-Range", content_range_header(r))
						self.end_headers()
						torrent_file.write(self.wfile, r)
					else: # this code is not tested
						import random, string
						boundary = '--------' + ''.join([ random.choice(string.letters) for i in range(32) ])
						self.send_header("Content-Type", "multipart/byteranges; boundary="+boundary)
						self.end_headers()
						for r in hr_parsed.range_specs:
							self.wfile.write(boundary+"\r\n")
							write_content_type()
							self.wfile.write("Content-Length: "+str(range_spec_len(r))+"\r\n")
							self.wfile.write("Content-Range: "+content_range_header(r)+"\r\n\r\n")
							torrent_file.write(self.wfile, r)
							self.wfile.write("\r\n"+boundary+"--\r\n")
					is_whole = False
				except httpheader.RangeUnsatisfiableError:
					self.send_response(416)
					self.send_common_header()
					self.send_header("Content-Range", "*/"+str(file_size))
					self.end_headers()
					is_whole = False
			except httpheader.ParseError:
				pass
		if is_whole:
			self.send_response(200)
			self.send_common_header()
			send_header_content_type()
			self.send_header("Content-Length", str(file_size))
			self.end_headers()
			r = httpheader.range_spec()
			r.first = 0
			r.last = file_size-1
			torrent_file.write(self.wfile, r)
	def do_GET(self):
		logger = logging.getLogger("root")
		try:
			logger.info(self.command)
			logger.info(self.path)
			logger.info(self.headers)
			piece_par_req = "/?piece_par="
			if "/"==self.path:
				self.send_response(200)
				self.send_common_header()
				self.send_header("Content-Type", "application/xhtml+xml")
				s = self.server.torrent.html_index
				self.send_header("Content-Length", str(len(s)))
				self.end_headers()
				self.wfile.write(s)
			elif self.path[0:len(piece_par_req)]==piece_par_req:
				self.send_response(200)
				self.send_common_header()
				self.send_header("Content-Type", "application/xhtml+xml")
				tag, value = coerce_piece_par(self.path[len(piece_par_req):])
				if 0==tag:
					logger.info("piece_par is set to "+str(value))
					self.server.piece_par.data = value
					s = "ok "+str(value)
				else:
					s = value
				s = self.server.torrent.write_html(s)
				self.send_header("Content-Length", str(len(s)))
				self.end_headers()
				self.wfile.write(s)
			else:
				torrent_file = self.server.torrent.find_file(self.path)
				if not torrent_file:
					self.send_response(404)
					self.end_headers()
				else:
					self.read_from_torrent(torrent_file)
		except IOError, e:
			logger.debug("IOError")
			raise e
		finally:
			logger.debug("exit the thread answering the request")

class http_server_bt2p(ThreadingMixIn, BaseHTTPServer.HTTPServer):
	pass

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
		self.st_uid = 0
		self.st_gid = 0
		self.st_size = 4096
		self.st_atime = 0
		self.st_mtime = 0
		self.st_ctime = 0

class BTFS(fuse.Fuse):
	def __init__(self, *args, **kw):
		print "Init"
		self.logger = logging.getLogger("root")
		fuse.Fuse.__init__(self, *args, **kw)

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

		self.logger.debug("ParsedFiles: " + unicode(self.files))

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
			logging.critical(str("".join(traceback.format_exception(*sys.exc_info()))))


	def read(self, path, size, offset):
		pass

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
			log_conf = "/etc/bittorrent2player/logging.conf" # configuration
		else:
			error_exit(229, "both \"--log\" and \"--log-conf\" are given, the logging configuration file name is ambiguous")
	if not (log_conf is None):
		logging.config.fileConfig(log_conf)
	else:
		logging.disable(logging.CRITICAL)

def main_default(options):
	if not ("domain-name" in options):
		options["domain-name"] = "127.0.0.1" # configuration
	if not ("port" in options):
		options["port"] = 17580 # configuration
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
	if "resume" in options:
		resume = options["resume"]
	else:
		resume = None
	resume_data = main_resume(resume)
	
	torrent_session = libtorrent.session()
	sp = libtorrent.session_settings()
	sp.request_timeout /= 10 # configuration
	sp.piece_timeout /= 10 # configuration
	sp.peer_timeout /= 20 # configuration
	sp.inactivity_timeout /= 20 # configuration
	torrent_session.set_settings(sp)
	torrent_session.listen_on(6881, 6891)
	
	e = libtorrent.bdecode(io.open(options["hash-file"], 'rb').read())
	torrent_info = libtorrent.torrent_info(e)
	torrent_descr = {"storage_mode": libtorrent.storage_mode_t.storage_mode_allocate
		, "save_path": options["save-path"]
		, "ti": torrent_info}
	if not (resume_data is None):
		torrent_descr["resume_data"] = resume_data
	logger.debug("the torrent description: "+str(torrent_descr))
	torrent_handle = torrent_session.add_torrent(torrent_descr)
	logger.debug("the torrent handle "+str(torrent_handle)+" is created")
	
	piece_par_ref0 = reference()
	piece_par_ref0.data = options["piece-par"]
	
	piece_server0 = piece_server()
	piece_server0.torrent_handle = torrent_handle
	piece_server0.torrent_info = torrent_info
	piece_server0.piece_par = piece_par_ref0
	piece_server0.init()

	alert_client0 = alert_client()
	alert_client0.torrent_session = torrent_session
	alert_client0.piece_server = piece_server0
	if "resume" in options:
		ra = resume_alert()
		ra.torrent_handle = torrent_handle
		
		rs = resume_save()
		rs.file_name = options["resume"]
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
	
	if "resume" in options:
		def f():
			logger.debug("shutting down with a resume")
			torrent_session.pause()
			rs.save(True)
		th.save_resume = f
	try:
		fs = BTFS(dash_s_do='setsingle')
		fs.parse(errex=1)
		fs.torrent_handle = torrent_handle
		fs.torrent_info = torrent_info
		fs.piece_server = piece_server0
		fs.parsebttree()
		fs.main()
		
	except KeyboardInterrupt:
		th.do()

def main_options(options, l, th):
	main_log(l)
	main_torrent_descr(options, th)

def main(argv=None):
	th = term_handler()
	signal.signal(signal.SIGTERM, th.hdo)
	if argv is None:
		argv = sys.argv
	try:
		crude_options, args = getopt.getopt(argv[1:], ""
			, ["resume=", "hash-file=", "save-path=", "piece-par=", "log", "log-conf=", "domain-name=", "port="])
	except getopt.error, error:
		error_exit(221, "the option "+error.opt+" is incorrect because "+error.msg)
	options = {}
	log = False
	log_conf = None
	for o, a in crude_options:
		if "--resume"==o:
			options["resume"] = a
		elif "--hash-file"==o:
			options["hash-file"] = a
		elif "--save-path"==o:
			options["save-path"] = a
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
		elif "--domain-name"==o:
			options["domain-name"] = a
		elif "--port"==o:
			options["port"] = a
		else:
			error_exit(223, "an unknown option is given")
	sys.argv[1:] = args
	main_options(options, (log, log_conf), th)

def main_daemon(argv=None):
	th = term_handler()
	signal.signal(signal.SIGTERM, th.hdo)
	if argv is None:
		argv = sys.argv
	access_mode = (7*8+5)*8+5
	def f(a):
		if not fs.exists(a):
			os.mkdir(a, access_mode)
		else:
			if not fs.isdir(a):
				error_exit(230, "can not open directory with a pid file")
	a = fs.join(os.environ["HOME"], ".local")
	f(a)
	a = fs.join(a, "var")
	f(a)
	a = fs.join(a, "run")
	f(a)
	ex_pid_file_name = fs.join(a, "bt2pd.pid") # configuration
	def scavenge_pid():
		os.unlink(ex_pid_file_name)
	if fs.exists(ex_pid_file_name):
		if fs.isfile(ex_pid_file_name):
			ex_pid = int(io.open(ex_pid_file_name, "r").read())
			success = False
			i = 10
			while (i>0 and not success):
				i -= 1
				try:
					os.kill(ex_pid, signal.SIGTERM)
				except OSError:
					success = True
				sleep(1)
			if not success:
				error_exit(231, "SIGTERM was sent to the existing process of this program, but it does not terminate")
			else:
				try:
					scavenge_pid()
				except OSError:
					pass
	if len(argv)==2:
		io.open(ex_pid_file_name, "w").write(unicode(os.getpid())+"\n")
		th.scavenge_pid = scavenge_pid
		def read_configuration():
			r = None
			try:
				r = json.load(io.open(fs.join(os.environ["XDG_CONFIG_HOME"], "bt2pd.conf")))["save_path"]
			except IOError:
				r = None
			except ValueError:
				r = None
			except KeyError:
				r = None
			return r
		save_path = read_configuration()
		if save_path is None:
			error_exit(232, "error while reading the configuration file")
		main_options({"hash-file": argv[1], "save-path": save_path.encode("utf8", "ignore")}, (False, None), th)
	elif len(argv)==1:
		sys.exit(os.EX_OK)
	else:
		error_exit(os.EX_USAGE, "more than 1 argument")

if __name__ == "__main__":
	main()
