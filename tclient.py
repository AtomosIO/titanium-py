import time
import urllib2
import json
import traceback
import string

clustersEndpoint = "/clusters/"
instancesEndpoint = "/instances/"
projectsEndpoint = "/projects/"
tokensEndpoint = "/tokens/"

class TClient:
	endpoint = ""
	token = ""
	pollingSleep = 1

	def __init__(self, endpoint="", token="", user="", password="", log=False):
		if len(endpoint) == 0:
			raise Error("Must specify endpoint")
			
		self.endpoint = endpoint
		
		if len(user) != 0 and len(password) != 0:
			# TODO Login and save token
			self.token = self.doMethodAndUnmarshal(tokensEndpoint, "POST", { 
				"user": user,
				"password": password,
			})["token"]
		else:
			self.token = token
	
	# Check to see if a request was successful
	def isSuccessful(self, request):
		if hasattr(request, "code"):
			if request["code"] == 1000000:
				return True
		
		if type(request) is dict:
			if request["code"] == 1000000:
				return True
				
		if type(request) is bool:
			return request
		
		return False
	
	def cleanUrl(self, url):
		return string.replace(string.replace(url, "//", "/"), "http:/", "http://")
	
	def doGetAndUnmarshal(self, url):
		targetUrl = self.cleanUrl(self.endpoint + url)
		request = urllib2.Request(targetUrl)
		request.add_header('Authorization', self.token)
		#print("GET: " + targetUrl)
		return json.load(urllib2.urlopen(request))
		
	# jsonObj must be a dict
	def doMethodAndUnmarshal(self, url, method, jsonObj):
		targetUrl = self.cleanUrl(self.endpoint + url)
		opener = urllib2.build_opener(urllib2.HTTPHandler)
		request = urllib2.Request(targetUrl, data=json.dumps(jsonObj))
		request.add_header('Authorization', self.token)
		request.get_method = lambda: method
		#print(method + ": " + targetUrl + " DATA->" + json.dumps(jsonObj))
		try:
			return json.load(opener.open(request))
		except urllib2.HTTPError, error:
			return json.load(error)
				
	def getInstance(self, id):
		return self.doGetAndUnmarshal(instancesEndpoint + str(id))	

	def getCluster(self, id):
		return self.doGetAndUnmarshal(clustersEndpoint + str(id))	
	
	def getRoot(self):
		return self.doGetAndUnmarshal("")	
		
	def createProject(self, projectName, public=True):
		return self.doMethodAndUnmarshal(projectsEndpoint, "POST", {
			"name": projectName,
			"public": public,
		})
		
	def createBatchCluster(self, name, project, interfaces):
		output = self.doMethodAndUnmarshal(clustersEndpoint, "POST", {
			"type": "batch",
			"name": name,
			"project": project,
			"interfaces": interfaces,
		})
		output["id"] = output["cluster_id"]
		return output
	
	def setProjectKernel(self, project, command, interfaces):
		return self.doMethodAndUnmarshal(projectsEndpoint+project, "PATCH", {
			"type": "kernel",
			"kernel": {
				"command": command,
				"interfaces": interfaces,
			}
		})
		
	def shutdownInstance(self, id):
		return self.doMethodAndUnmarshal(instancesEndpoint+str(id), "PATCH", {
			"shutdown": True,
		})		
	
	def waitForInstanceFinish(self, id, timeout=60):
		expireTime = int(time.time()) + timeout
		
		while int(time.time()) < expireTime:
			instance = self.getInstance(id)
			if instance["status"] == "Stopped":
				return True
			time.sleep(self.pollingSleep)
		
		raise Exception("Timeout while waiting for instance " + str(id))

	def waitForInstanceStart(self, id, timeout=60):
		expireTime = int(time.time()) + timeout
		
		while int(time.time()) < expireTime:
			instance = self.getInstance(id)
			if instance["status"] == "Active" or instance["status"] == "Stopped":
				return True
			time.sleep(self.pollingSleep)
		
		raise Exception("Timeout while waiting for instance " + str(id))

	def waitForClusterFinish(self, id, timeout=60):
		expireTime = int(time.time()) + timeout
		
		while int(time.time()) < expireTime:
			cluster = self.getCluster(id)
			if cluster["status"] == "Stopped":
				return True
			time.sleep(self.pollingSleep)
		
		raise Exception("Timeout while waiting for cluster " + str(id))
