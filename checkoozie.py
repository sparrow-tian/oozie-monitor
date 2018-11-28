import urllib2 
import os
import json 
import sys 
import datetime 
import time 
import requests
import logging

global app_name_list
app_name_list=[]
oozie_server_ip='your oozie server IP'
#for some on purpose killed jobs no need to check status
badcoordlist=['coordinator_name_1','coordinator_name_2']
#check workflow status
def checkstatus(app_name, job):
	url = "http://%s:11000/oozie/v1/job/%s?show=info" % (oozie_server_ip,job)
	req = urllib2.Request(url) 
	response = urllib2.urlopen(req) 
	output = response.read() 
	data = json.loads(output) 
	actionslist = []
	status = data['status']
	workflowname = data['appName']
	if status != 'SUCCEEDED':
		workflow_msg = "Workflow Name :%s\nStatus:\t%s\nJobid:\t%s" % (workflowname, status, job)
		if app_name not in app_name_list:
			msg = "\n%s\n%s" % (app_name, workflow_msg)
			app_name_list.append(app_name)
		else:
			msg = workflow_msg
		print msg
	else:
		printed = False
		for i in data['actions']:
                    #Some workflow may contain sub-workflow.
                    if i['type']=='sub-workflow':
                        checkstatus(app_name,i['externalId'])
                    else:
			if i['status'] != 'OK':
				if not printed:
					msg = "\n%s\nWorkflow Name :%s" % (app_name, workflowname) if app_name not in app_name_list else "Workflow Name :%s" % (workflowname)
					print msg
					printed = True
				print i['id']+'\t\t'+i['status']
		if printed: 
			app_name_list.append(app_name) 

#Get workflow id from coordinator	
def getworkjob(app_name, coordjob):
	dt = datetime.datetime.strptime(date,'%Y-%m-%d')
        #Oozie server use GMT timezone and our localtime zone is GMT+8, this is use to monitor job running in yesterday.
	startdt = dt + datetime.timedelta(days=-1,hours=14)
	enddt = startdt + datetime.timedelta(hours=24)
	url = "http://%s:11000/oozie/v1/job/%s?show=info&order=desc" % (oozie_server_ip,coordjob) 
	req = requests.get(url)
    	data = json.loads(req.text)
	status = data['status']
	workjob_list = []
	if status == 'RUNNING':
		for work in data['actions']:
			nomdt = datetime.datetime.strptime(work['nominalTime'].split(',')[1].split('GM')[0].strip(),'%d %b %Y %H:%M:%S')
			if nomdt >= startdt and nomdt <= enddt:
				if work['externalId'] is not None:
					checkstatus(app_name,work['externalId'])
					break
				else:
					if app_name not in workjob_list:
						print 
						print app_name
						print work['id'],work['status']
						workjob_list.append(app_name)
					else:
						print work['id'],work['status']
	elif coordjob not in badcoordlist:
		print 
		print app_name
		print coordjob,status
		

#Get coordinator id from bundle or coordinator list
def main():
	for line in open("%s" % (filename)):
		app_name = line.split('\t')[0].strip()
		job = line.split('\t')[1].strip()
		if job[-1:] == 'C':
			getworkjob(app_name, job)
		else:
			req=requests.get("http://%s:11000/oozie/v1/job/%s?show=info" % (oozie_server_ip,job))
			data = json.loads(req.text)
			coordjoblist=[]
			for coordjoball in data['bundleCoordJobs']:
				getworkjob(app_name, coordjoball['coordJobId'])


if __name__ == "__main__":
    date = sys.argv[1]
    filename = sys.argv[2]
 
    main()
