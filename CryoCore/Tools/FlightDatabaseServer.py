#!/usr/bin/env python
# Simple script to access flight database via web, loosely
# based on existing web-code.
# Note: Tabs used for indentation.
# Written to be as self-contained as possible, with lots of assumptions.
# By Daniel Stodle, daniel@norut.no, 2012

import os, sys, MySQLdb, threading, time
import http.server, socketserver, json, base64

db								= None
simulateLiveMode				= False
lastRequestTime					= 0.0
liveModeTimeOffset				= 1.0
lastLiveModeTimeOffsetAdjust	= 0.
verbose							= False

class MVDatabase:
	def __init__(s, dbHost="localhost",dbUser="pilot",dbPasswd="pi10t",dbName="uav"):
		s.host			= dbHost
		s.user			= dbUser
		s.passwd		= dbPasswd
		s.lock			= threading.Lock()
	
	def getNewConnection(s, name=""):
		return MySQLdb.connect(host=s.host,user=s.user, passwd=s.passwd,db=name, use_unicode=True,charset="utf8")
	
	def execute(s, SQL, name="", params=[]):
		SQL = SQL.replace("?", "%s")
		for i in range(0,3):
			try:
				conn		= s.getNewConnection(name)
				cursor		= conn.cursor()
				cursor.execute(SQL, params)
				return cursor
			except MySQLdb.OperationalError as e:
				raise e
	
	def getMinimumTimestamp(s, database):
		try:
			cursor = s.execute("SELECT MIN(timestamp) FROM status;", database)
			return cursor.fetchone()[0]
		except:
			try:
				cursor = s.execute("SELECT MIN(timestamp) FROM autopilot;", database)
				return cursor.fetchone()[0]
			except:
				return 0
	
	def getMaximumTimestamp(s, database):
		try:
			cursor = s.execute("SELECT MAX(timestamp) FROM status;", database)
			return cursor.fetchone()[0]
		except:
			try:
				cursor = s.execute("SELECT MAX(timestamp) FROM autopilot;", database)
				return cursor.fetchone()[0]
			except:
				return 0
	
	def getMinMaxTimestamp(s, database):
		minTs		= s.getMinimumTimestamp(database)
		actualMaxTs	= s.getMaximumTimestamp(database)
		if simulateLiveMode and 0:
			maxTs	= minTs + liveModeTimeOffset
			if maxTs > actualMaxTs:
				maxTs	= actualMaxTs
		else:
			maxTs	= actualMaxTs
		return {"minTimestamp":minTs, "maxTimestamp":maxTs}
	
	def getDatabases(s):
		cursor		= s.execute("show databases;")
		databases	= []
		info		= {}
		while True:
			row		= cursor.fetchone()
			if row == None:
				break
			# My MSQL is broken. Can't get rid of the "test" db. Filter it out.
			if row[0] == "information_schema" or row[0] == "test":
				continue
			databases.append(row[0])
		for db in databases:
			dbInfo	= {"hasImuView":False, "hasAutopilotLog":False, "hasRemoteStatusInfo":False, "hasTrimbleGPS":False, "hasLaser":False, "hasTrios":False}
			cursor	= s.execute("show tables;", db)
			tuples	= cursor.fetchall()
			rows	= []
			for t in tuples:
				rows.append(t[0])
			if "imu_view" in rows:
				dbInfo["hasImuView"]			= True
			if "autopilot" in rows:
				dbInfo["hasAutopilotLog"]		= True
			if "RemoteStatusInfo" in rows:
				dbInfo["hasRemoteStatusInfo"]	= True
			if "trimble" in rows:
				dbInfo["hasTrimbleGPS"]			= True
			if "laser" in rows:
				dbInfo["hasLaser"]				= True
			try:
				cursor	= s.execute("select name from status_channel;", db)
				tuples	= cursor.fetchall()
				rows	= []
				for t in tuples:
					rows.append(t[0])
				#if u"RemoteStatusInfo" in rows:
				#	dbInfo["hasRemoteStatusInfo"]	= True
			except:
				print("No status_channel table for database", db)
			try:
				cursor	= s.execute("select count(*) from trios_sample;", db)
				result	= cursor.fetchall()
				if int(result[0][0]) > 0:
					dbInfo["hasTrios"] = True
			except Exception as e:
				print(e)
			info[db]	= dbInfo
		return info
	
	def getChannelsAndParameters(s, database):
		try:
			cursor		= s.execute("SELECT chanid, name FROM status_channel ORDER BY name", database)
			s.channels	= {}
			for row in cursor.fetchall():
				channel	= {"name":row[1], "params":{}}
				s.channels[row[0]]	= channel
			
			cursor = s.execute("SELECT paramid, name, chanid FROM status_parameter ORDER BY name", database)
			while True:
				row		= cursor.fetchone()
				if row == None:
					break
				param	= {"name":row[1]}
				channel	= s.channels[row[2]]
				channel["params"][row[0]]	= param
			return s.channels
		except Exception as e:
			print(e)
			return {}
	
	def convertToNativeType(s, item):
		if isinstance(item, str):
			# Figure out what this is.. a string? int() accepts only integers, float takes stuff like 1.2 and 1e4. If either fails, it's a string.
			try:
				item	= int(item)
			except ValueError:
				try:
					item	= float(item)
				except ValueError:
					pass
		return item
	
	def getRows(s, cursor, convertTypes=False):
		""" Returns a list of rows from the given cursor, converting strings to native types (float, int)
			if convertTypes == True. None-values are replaced with "?" and values identical to the previous
			value in the previous row replaced with "*". This is used by the visualizer to avoid inserting
			new values in the interpolator where no new value actually exists. This is important for correctly
			interpolating lat/lon, for instance.
		"""
		list		= []
		prev		= []
		while True:
			row		= cursor.fetchone()
			if row == None:
				break
			item	= [x for x in row]
			if len(prev) > 0:
				for i in range(0,len(item)):
					if row[i] == None:
						item[i]	= "?"
					elif row[i] == prev[i]:
						item[i]	= "*"
					else:
						prev[i]	= item[i]
					if convertTypes:
						item[i]	= s.convertToNativeType(item[i])
			else:
				if convertTypes:
					for i in range(0,len(item)):
						item[i]	= s.convertToNativeType(item[i])
				prev	= [x for x in item]
			list.append(item)
		return list
	
	def getParameterData(s, database, cid, pid, firstTimestamp=None, lastTimestamp=None):
		params	= [cid, pid]
		query	= [	"select timestamp, value from status where chanid=? and paramid=? order by timestamp;",
					"select timestamp, value from status where chanid=? and paramid=? and timestamp >? order by timestamp;",
					"select timestamp, value from status where chanid=? and paramid=? and timestamp >? and timestamp <=? order by timestamp;" ]
		if firstTimestamp != None:
			params.append(firstTimestamp)
			if lastTimestamp != None:
				params.append(lastTimestamp)
		try:
			cursor	= s.execute(query[len(params)-2], database, params)
			# We need ints and floats converted to native types for items fetched from the status table.
			return s.getRows(cursor, True)
			list		= []
			prev		= None
			while True:
				row		= cursor.fetchone()
				if row == None:
					break
				ts		= row[0]
				item	= row[1]
				if prev != None:
					if prev == item:
						item	= "*"
					prev		= item
				else:
					prev		= item
				if item == None:
					item		= "?"
				# Figure out what this is.. a string? int() accepts only integers, float takes stuff like 1.2 and 1e4. If either fails, it's a string.
				try:
					item		= int(item)
				except ValueError:
					try:
						item	= float(item)
					except ValueError:
						pass
				list.append([ts, item])
			return list
		except Exception as e:
			print(e)
			return []
	
	def getIMUView(s, database, firstTimestamp=None, lastTimestamp=None):
		params	= []
		query	= [	"select timestamp,lat,lon,alt,q0,q1,q2,q3,vx,vy,vz,ax,ay,az from imu_view order by timestamp;",
					"select timestamp,lat,lon,alt,q0,q1,q2,q3,vx,vy,vz,ax,ay,az from imu_view where timestamp>? order by timestamp;",
					"select timestamp,lat,lon,alt,q0,q1,q2,q3,vx,vy,vz,ax,ay,az from imu_view where timestamp>? and timestamp<=? order by timestamp;" ]
		if firstTimestamp != None:
			params.append(firstTimestamp)
			if lastTimestamp != None:
				params.append(lastTimestamp)
		try:
			cursor		= s.execute(query[len(params)], database, params)
			return s.getRows(cursor)
		except Exception as e:
			print(e)
			return []
	
	def getTrimbleGPS(s, database, firstTimestamp=None, lastTimestamp=None):
		params	= []
		query	= [	"select timestamp,lat,lon,alt,hdg,avr_yaw,avr_tilt,spd from trimble order by timestamp;",
					"select timestamp,lat,lon,alt,hdg,avr_yaw,avr_tilt,spd from trimble where timestamp>? order by timestamp;",
					"select timestamp,lat,lon,alt,hdg,avr_yaw,avr_tilt,spd from trimble where timestamp>? and timestamp<=? order by timestamp;" ]
		if firstTimestamp != None:
			params.append(firstTimestamp)
			if lastTimestamp != None:
				params.append(lastTimestamp)
		try:
			cursor		= s.execute(query[len(params)], database, params)
			return s.getRows(cursor)
		except Exception as e:
			print(e)
			return []

	def getLaser(s, database, firstTimestamp=None, lastTimestamp=None):
		params	= []
		query	= [	"select timestamp,distance,`signal` from laser order by timestamp;",
					"select timestamp,distance,`signal` from laser where timestamp>? order by timestamp;",
					"select timestamp,distance,`signal` from laser where timestamp>? and timestamp<=? order by timestamp;" ]
		if firstTimestamp != None:
			params.append(firstTimestamp)
			if lastTimestamp != None:
				params.append(lastTimestamp)
		try:
			cursor		= s.execute(query[len(params)], database, params)
			return s.getRows(cursor)
		except Exception as e:
			print(e)
			return []
	
	def getTrios(s, database, firstTimestamp=None, lastTimestamp=None):
		params	= []
		query	= [	"select timestamp,instrument,integration_time,payload from trios_sample order by timestamp;",
					"select timestamp,instrument,integration_time,payload from trios_sample where timestamp>? order by timestamp;",
					"select timestamp,instrument,integration_time,payload from trios_sample where timestamp>? and timestamp<=? order by timestamp;" ]
		if firstTimestamp != None:
			params.append(firstTimestamp)
			if lastTimestamp != None:
				params.append(lastTimestamp)
		try:
			cursor		= s.execute(query[len(params)], database, params)
			rows		= s.getRows(cursor)
			print("%d trios rows" % (len(rows)))
			for row in rows:
				if len(row[3]) > 1:
					# TODO: Maybe use base64 to send this data. It's easy enough on the python-side of things
					#       but full of annoying issues on the receiving side.
					decodedArray	= []
					for i in range(0,len(row[3]),2):
						low		= ord(row[3][i])
						high	= ord(row[3][i+1])
						decodedArray.append(int((high << 8) | low))
					row[3]	= decodedArray
			return rows
		except Exception as e:
			print(e)
			return []
		
	
	def getAutopilotLog(s, database, firstTimestamp=None, lastTimestamp=None):
		params	= []
		query	= [	"select timestamp,lat,lon,alt,yaw,pitch,roll,speed from autopilot order by timestamp;",
					"select timestamp,lat,lon,alt,yaw,pitch,roll,speed from autopilot where timestamp>? order by timestamp;",
					"select timestamp,lat,lon,alt,yaw,pitch,roll,speed from autopilot where timestamp>? and timestamp<=? order by timestamp;" ]
		if firstTimestamp != None:
			params.append(firstTimestamp)
			if lastTimestamp != None:
				params.append(lastTimestamp)
		try:
			cursor		= s.execute(query[len(params)], database, params)
			return s.getRows(cursor)
		except Exception as e:
			print(e)
			return []
	
	def getRemoteStatus(s, database, firstTimestamp=None, lastTimestamp=None):
		params	= []
		query	= [	"select timestamp,lat,lon,alt,yaw,speed from RemoteStatusInfo order by timestamp;",
					"select timestamp,lat,lon,alt,yaw,speed from RemoteStatusInfo where timestamp>? order by timestamp;",
					"select timestamp,lat,lon,alt,yaw,speed from RemoteStatusInfo where timestamp>? and timestamp<=? order by timestamp;" ]
		if firstTimestamp != None:
			params.append(firstTimestamp)
			if lastTimestamp != None:
				params.append(lastTimestamp)
		try:
			cursor		= s.execute(query[len(params)], database, params)
			return s.getRows(cursor)
		except Exception as e:
			print(e)
			return []
	
	def toJSON(s, data):
		return json.dumps(data, separators=(',',':'))
	
	def toPrettyJSON(s, data):
		return json.dumps(data, sort_keys=True, indent=2)
	

"""
Some notes regarding "expected" usage:
1) Visualizer calls getMinimumTimestamp and getMaximumTimestamp [probably through the same call]
2) Visualizer prefetches data for ... say, 10-second intervals by starting at the minimum timestamp
   (or elsewhere, depending)

For instance:
get /database/minMaxTimestamp
	returns min and max timestamp

get /database/channelsAndParams
	returns a mapping of channel names and params to cids and pids; enable visualization of additional data not from IMU

get /database/imu[/firstTimestamp[/lastTimestamp]
	return a window of data for the given timestamp region, or everything
	if the table doesn't exist (we are running live against the basestation), an error is returned. This will
	cause the visualizer to request remoteStatus instead (see below).

get /database/autopilot[/firstTimestamp[/lastTimestamp]
	returns the autopilot log (which must be manually imported into the database as timestamp, lat, lon, alt, yaw, pitch, roll, speed)

get /remoteStatus[/firstTimestamp[/lastTimestamp]]
	returns the same data as imu, but without orientation and with lower-frequency updates.

Periodically:
get /minMaxTimestamp
	returns current min and max timestamp. If we're running live, this will keep updating.

get /databases
	return a list of databases, along with some properties: hasImuView, hasAutopilotLog, hasRemoteStatusInfo.

"""

class RequestHandler(http.server.BaseHTTPRequestHandler):
	def do_GET(s):
		global lastRequestTime, liveModeTimeOffset, lastLiveModeTimeOffsetAdjust
		try:
			args		= s.path.split("/")[1:]
			print(s.path, args)
			contentType	= "text/plain"
			database	= args[0]
			query		= args[1] if len(args) > 1 else None
			params		= args[2:]
			
			if query != None:
				if query == "imu":
					firstTimestamp	= params[0] if len(params) > 0 else None
					lastTimestamp	= params[1] if len(params) > 1 else None
					data			= db.toJSON(db.getIMUView(database, firstTimestamp, lastTimestamp))
				elif query == "trimble":
					firstTimestamp	= params[0] if len(params) > 0 else None
					lastTimestamp	= params[1] if len(params) > 1 else None
					data			= db.toJSON(db.getTrimbleGPS(database, firstTimestamp, lastTimestamp))
				elif query == "trios":
					firstTimestamp	= params[0] if len(params) > 0 else None
					lastTimestamp	= params[1] if len(params) > 1 else None
					data			= db.toJSON(db.getTrios(database, firstTimestamp, lastTimestamp))
				elif query == "laser":
					firstTimestamp	= params[0] if len(params) > 0 else None
					lastTimestamp	= params[1] if len(params) > 1 else None
					data			= db.toJSON(db.getLaser(database, firstTimestamp, lastTimestamp))
				elif query == "remoteStatus":
					firstTimestamp	= params[0] if len(params) > 0 else None
					lastTimestamp	= params[1] if len(params) > 1 else None
					data			= db.toJSON(db.getRemoteStatus(database, firstTimestamp, lastTimestamp))
				elif query == "minMaxTimestamp":
					if simulateLiveMode and 0:
						if time.time() - lastRequestTime > 10.0:
							# Reset the live offset clock, this is probably another run.
							liveModeTimeOffset				= 7109	# This is the timestamp at which the action begins in the Snarby crash flight.
							lastLiveModeTimeOffsetAdjust	= time.time()
						elif time.time() - lastLiveModeTimeOffsetAdjust > 1:
							liveModeTimeOffset				+= time.time() - lastLiveModeTimeOffsetAdjust
							lastLiveModeTimeOffsetAdjust	= time.time()
					data			= db.toJSON(db.getMinMaxTimestamp(database))
					lastRequestTime	= time.time()
				elif query == "maxTimestamp":
					data			= db.toJSON({"maxTimestamp":db.getMaximumTimestamp(database)})
				elif query == "minTimestamp":
					data			= db.toJSON({"minTimestamp":db.getMinimumTimestamp(database)})
				elif query == "channelsAndParams":
					data			= db.toJSON(db.getChannelsAndParameters(database))
				elif query == "channelsAndParamsPretty":
					data			= db.toPrettyJSON(db.getChannelsAndParameters(database))
				elif query == "autopilot":
					firstTimestamp	= params[0] if len(params) > 0 else None
					lastTimestamp	= params[1] if len(params) > 1 else None
					data			= db.toJSON(db.getAutopilotLog(database, firstTimestamp, lastTimestamp))
				elif query == "paramValue":
					try:
						cid				= int(params[0])
						pid				= int(params[1])
						firstTimestamp	= params[2] if len(params) > 2 else None
						lastTimestamp	= params[3] if len(params) > 3 else None
						data			= db.toJSON(db.getParameterData(database, cid, pid, firstTimestamp, lastTimestamp))
					except Exception as e:
						s.send_error(404, "Format: paramValue/chanID/paramID[/firstTimestamp[/secondTimestamp]]\n%s" % (e))
						return
				else:
					s.send_error(404, "Unknown query %s" % s.path)
					return
			elif database == "databases":
				data			= db.toJSON(db.getDatabases())
			else:
				s.send_error(404, "Specify a database and a query, for instance uav/minMaxTimestamp. Invalid: %s" % s.path)
				return
			s.send_response(200)
			s.send_header("Content-type", contentType)
			s.end_headers()
			s.wfile.write(data)
			s.wfile.close()
			return
		except IOError:
			s.send_error(404, "Not found: %s" % path)
	
	def log_message(s, format, *args):
		if verbose:
			print(format % (args))

class ThreadedHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer): 
	pass
	

def usage(name):
	print("Usage: %s [-s, --simulateLive]" % (name))
	print("Simulate live enables testing of steadily increasing timestamps.")

if __name__ == "__main__":
	db		= MVDatabase()
	if len(sys.argv) > 1:
		for arg in sys.argv[1:]:
			if arg == "--simulateLive" or arg == "-s":
				simulateLiveMode	= True
			elif arg == "--verbose" or arg == "-v":
				verbose				= True
			else:
				usage(sys.argv[0])
				sys.exit(1)
	print("Flight database server running.. Control-C to stop.")
	server	= ThreadedHTTPServer(('',8001), RequestHandler) 
	server.serve_forever() 
	
