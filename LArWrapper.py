"""! @brief Art logfile parser """
##
# @mainpage LArWrapper.py
#
# @section description_main A wrapper for lar that parses logs and finds unprocessed files.
#
# Copyright (c) 2022 Heidi Schellman, Oregon State University
##
# @file LArWrapper.py


from argparse import ArgumentParser as ap
import sys
import os
import subprocess
import time
import datetime
import requests
import Loginator

class LArWrapper:
    def __init__(self,fcl=None,replicas=None,flist="",o="temp.root",n=None,nskip=0,appFamily=None, appName=None,
     appVersion=None, deliveryMethod=None, workflowMethod=None, projectID=None, sam_web_uri=None,processID=None,\
     processHASH=None,formatString="runLar_%s_%s_%%tc_%s_%s_%s.root", dataTier="sam-user", dataStream="test"):
        self.fcl = fcl
        self.flist = flist
        self.n = n
        self.nskip = nskip
        self.o = o
        self.appFamily = appFamily
        self.appVersion = appVersion
        self.appName = appName
        self.deliveryMethod = deliveryMethod
        self.workflowMethod = workflowMethod
        self.oname = None
        self.ename = None
        self.formatString = formatString
        self.replicas = replicas
        self.flist = flist
        self.returncode = None
        self.projectID = projectID
        self.sam_web_uri = sam_web_uri
        self.processID = processID
        self.processHASH = processHASH
        self.dataTier = dataTier
        self.dataStream = dataStream

        if self.formatString == None:
            formatString = "process_%s_%s_%%tc_%s_%s_%s.root"


    def DoLAr(self,cluster=0,process=0):
        print ("check fcl",self.fcl,os.path.exists(self.fcl))
        print ("reading",self.flist)
        stamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S%Z")
        print (self.formatString)
        print (self.projectID, cluster, process, os.path.basename(self.fcl).replace(".fcl",""))
        fname = self.formatString%(self.deliveryMethod,self.projectID, cluster, process, os.path.basename(self.fcl).replace(".fcl",""))
        self.oname = fname.replace(".root",".out").replace("%tc",stamp)
        self.ename = fname.replace(".root",".err").replace("%tc",stamp)
        ofile = open(self.oname,'w')
        efile = open(self.ename,'w')

        if self.deliveryMethod == "dd":
            cmd = 'lar -c %s -s %s -n %i --nskip %i -o %s --sam-data-tier %s --sam-stream-name %s'%(self.fcl, self.flist, self.n, self.nskip, self.o, self.dataTier, self.dataStream)
            print ("cmd = ",cmd)
            proc = subprocess.run(cmd, shell=True, stdout=ofile,stderr=efile)
        elif self.deliveryMethod == "samweb":
            lar_cmd = 'lar -c %s -n %i --sam-web-uri=%s --sam-process-id=%s --sam-application-family=%s \
             --sam-application-version=%s --sam-data-tier %s --sam-stream-name %s'%(self.fcl,\
             self.n,self.sam_web_uri,self.processID,self.appFamily,self.appVersion,self.dataTier,self.dataStream)
            print (lar_cmd)
            proc = subprocess.run(lar_cmd,shell=True, stdout=ofile,stderr=efile)

        else:  # assume it's something like interactive
            cmd = 'lar -c %s -s %s -n %i --nskip %i -o %s'%(self.fcl, self.flist, self.n, self.nskip, fname)
            print ("cmd = ",cmd)
            proc = subprocess.run(cmd, shell=True, stdout=ofile,stderr=efile)
        self.returncode = proc.returncode
        ofile.close()
        efile.close()
        return self.returncode

    def LArResults(self):
        # get log info, match with replicas
        logparse = Loginator.Loginator(self.oname)

        logparse.readme()  # get info from the logfile

        info = {"application_family":self.appFamily,"application_name":self.appName, "application_version":self.appVersion,"delivery_method":self.deliveryMethod,"workflow_method":self.workflowMethod,"project_id":self.projectID}
        print ("delivery",self.deliveryMethod)
        if self.deliveryMethod == "dd":

            info["dd_worker_id"]=self.processHASH

            if self.replicas != None:
                unused_replicas = logparse.addreplicainfo(self.replicas)
            else:
                unused_replicas = []
            unused_files = []
            for u in unused_replicas:
                unused_files.append(u["namespace"]+":"+u["name"])
            logparse.addmetacatinfo()
            print ("files not used",unused_files)
        elif self.deliveryMethod == "samweb":
            #unused_files = logparse.findmissingfiles(self.files)
            info["process_id"]=self.processID
            logparse.addsaminfo()

        else:
            print ("unknown delivery mechanism")


        logparse.addinfo(info)
        logparse.addsysinfo()

        # write out json files for processed files whether closed properly or not.  Those never opened don't get logged.
        logparse.writeme()
        return unused_files

if __name__ == "__main__":
    parser = ap()
    parser.add_argument('--delivery_method',required=True, type=str, help='["samweb","dd","wfs"]')
    parser.add_argument('--workflow_method',type=str, help='["samweb","dd","wfs"]')
    parser.add_argument('--processHASH',type=str, default="", help='string code generated by dd for worker')
    parser.add_argument('--processID',type=int, default=0, help='processID generated by samweb')
    parser.add_argument('--sam_web_uri',type=str, help='samweb url for the project')
    parser.add_argument('--appFamily', default='test',type=str, help='samweb needs this')
    parser.add_argument('--appName', default='test', type=str, help='samweb needs this')
    parser.add_argument('--appVersion', type=str, help='samweb needs this')
    parser.add_argument('--dataTier', type=str, help='data tier for output file if only one')
    parser.add_argument('--dataStream', type=str, help='data stream for output file if only one')
    parser.add_argument('-o', default="temp.root", type=str, help='output root file')
    parser.add_argument('-c', required=True, type=str, help='name of fcl file')
    parser.add_argument('--user', type=str, help='user name')
    parser.add_argument('--projectID', type=int, default=0, help='integer that identifies the project, samweb/dd/wfs')
    parser.add_argument('--timeout', type=int, default=120)
    parser.add_argument('--wait_time', type=int, default=120)
    parser.add_argument('--wait_limit', type=int, default=5)
    parser.add_argument('-n', type=int, default=-1, help='number of events total to process')
    parser.add_argument('--nskip', type=int, default=0, help='number of events to skip before starting')
    parser.add_argument('--formatString', type = str, default='runLar_%s_%s_%%tc_%s_%s_%s.root',help='format string used by LarWrapper for logs')
    args = parser.parse_args()

    print (args)

    if args.processHASH == None and "MYWORKERID" in os.environ:
        args.processHASH = os.environ("MYWORKERID")
    lar = LArWrapper(fcl=args.c, n=args.n, nskip = args.nskip, appFamily=args.appFamily,appName=args.appName,
    appVersion=os.getenv("DUNESW_VERSION"), deliveryMethod=args.delivery_method, workflowMethod=args.workflow_method,
    processID = args.processID, processHASH = args.processHASH, projectID=args.projectID, sam_web_uri = args.sam_web_uri, formatString=args.formatString)
    returncode = lar.DoLAr(0, args.processID)
    unused_files = lar.LArResults()
    sys.exit(returncode)