"""Wrapper for LAr that supports both sam and dd access """
##
# @mainpage LArWrapper.py
#
# @section description_main A wrapper for lar that uses loginator to parse logs and finds unprocessed files.
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

## class that wraps lar - tries to have similar arguments.

class LArWrapper:
    ''' LArWrapper - generic wrapper for LArSoft that works with samweb and dd '''
    def __init__(self,debug=False,fcl=None,flist="",o="temp*.root",n=None,nskip=0,appFamily=None, appName=None,\
     appVersion=None,  projectID=None, sam_web_uri=None,processID=None,dataTier="sam-user", dataStream="test",\
     deliveryMethod=None, workflowMethod=None,formatString="runLar_%s_%s_%%tc_%s_%s_%s.root",\
     processHASH=None,replicas=None):

         """

         Many of the options are the same as those for lar

         .. code-block:: shell

                $lar -h #for more examples

         :param debug: add printout and send stdout to console instead of a files
         :type debug: bool

         :param fcl: LAr option - fcl file to use
         :type dataset: str

         :param flist: LAr/dd option - list of space delimited files to process from dd
         :type lar_limit: str

         :param appFamily: LAr option - Optional Information about process
         :type appFamily: str

         :param appVersion:  LAr option - Optional Information about process
         :type appVersion: str

         :param appName:  LAr option - Optional Information about process
         :type appName: str

         :param projectID: LAr option - DD or samweb project ID
         :type projectID: int

         :param sam_web_uri: LAr/samweb option - samweb project url
         :type sam_web_uri: str

         :param processID: LAr/samweb option - samweb process ID
         :type processID: int

         :param processHASH: dd option - process ID from dd
         :type processHASH: str

         :param dataTier: LAr option - data tier for output
         :type dataTier: str

         :param dataStream: LAr option - data strem for output
         :type dataStream: str

         :param deliveryMethod: required parameter to tell samweb from dd/wfs
         :type deliveryMethod: str

         :param workflowMethod: optional parameter - batch/interactive/wfs
         :type workflowMethod: str

         :param formatString: optional format the output log names
         :type formatString: str
         :return: the lar return code
         :rtype: int
         """
     # lar arguments
         self.fcl = fcl
         self.flist = flist
         self.n = n
         self.nskip = nskip
         self.o = o
         self.appFamily = appFamily
         self.appVersion = appVersion
         self.appName = appName
         self.projectID = projectID
         ## useful for samweb
         self.sam_web_uri = sam_web_uri
         self.processID = processID
         self.dataTier = dataTier
         self.dataStream = dataStream
         ## useful debuging
         self.deliveryMethod = deliveryMethod
         self.workflowMethod = workflowMethod
         self.oname = None
         self.ename = None
         self.returncode = None
         self.debug = debug
        # useful for dd
         self.formatString = formatString
         self.replicas = replicas
         self.processHASH = processHASH

        ## the format string is used to create an output file name for stdout and err
         if self.formatString == None:
            formatString = "process_%s_%s_%%tc_%s_%s_%s.root"

   ## actually run lar
    def DoLAr(self,cluster=0,process=0):
        ''' Method to run the LAr code

        :param cluster: batch cluster
        :type cluster: int

        :param process: batch process
        :type process: int

        :return: lar return code
        :return: int

        '''
        stamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S%Z")
        fclname=os.path.basename(self.fcl)
        if self.debug:
            print ("check fcl",self.fcl,os.path.exists(self.fcl))
            print ("reading",self.flist)
            print (self.formatString)
            print (self.projectID, cluster, process, os.path.basename(self.fcl).replace(".fcl",""))
        fname = self.formatString%(self.deliveryMethod,self.projectID, cluster, process, os.path.basename(self.fcl).replace(".fcl",""))
        self.oname = fname.replace(".root",".out").replace("%tc",stamp)
        self.ename = fname.replace(".root",".err").replace("%tc",stamp)
        ofile = open(self.oname,'w')
        efile = open(self.ename,'w')

        # data dispatcher case
        if self.deliveryMethod == "dd":
            cmd = 'lar -c %s -s %s -n %i --nskip %i -o %s --sam-data-tier %s --sam-stream-name %s'%(self.fcl, self.flist, self.n, self.nskip, self.o, self.dataTier, self.dataStream)
            print ("LArWrapper dd cmd = ",cmd)
            proc = subprocess.run(cmd, shell=True, stdout=ofile,stderr=efile)

        # samweb case
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

## here we get some information about how things went from the log file and other sources
    def LArResults(self):
        print("check debug for LarWrapper",self.debug)
        # get log info, match with replicas
        logparse = Loginator.Loginator(logname=self.oname,debug=self.debug)
        logparse.readme()  # get info from the logfile
# build up some information
        info = {"application_family":self.appFamily,"application_name":self.appName, "application_version":self.appVersion,\
        "delivery_method":self.deliveryMethod,"workflow_method":self.workflowMethod,"project_id":self.projectID,"fcl":os.path.basename(self.fcl)}
        if self.debug: print ("delivery method",self.deliveryMethod)
        if self.deliveryMethod == "dd":
            info["dd_worker_id"]=self.processHASH
            # dig around in replicas and see if we didn't use some of them, if so, tell the calling program
            if self.replicas != None:
                unused_replicas = logparse.addreplicainfo(self.replicas)
            else:
                unused_replicas = []
            unused_files = []
            for u in unused_replicas:
                unused_files.append(u["namespace"]+":"+u["name"])

            # ask metacat for info about each file
            logparse.addmetacatinfo()
            print ("files not used",unused_files)

        # samweb info if not using dd
        elif self.deliveryMethod == "samweb":
            #unused_files = logparse.findmissingfiles(self.files)
            info["process_id"]=self.processID
            logparse.addsaminfo()
            unused_files = None

        # heck if I know what to do.
        else:
            print ("unknown delivery mechanism")

        # add in the information from the system and wrapper initialization
        logparse.addinfo(info)
        logparse.addsysinfo()

        # write out json files for processed files whether closed properly or not.  Those never opened don't get logged.
        logparse.writeme()
        return unused_files

    def main():
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
        parser.add_argument('--debug', type=bool, default=False)
        args = parser.parse_args()

        print (args)

        if args.processHASH == None and "MYWORKERID" in os.environ:
            args.processHASH = os.environ("MYWORKERID")
        lar = LArWrapper(fcl=args.c, n=args.n, nskip = args.nskip, appFamily=args.appFamily,appName=args.appName,
        appVersion=os.getenv("DUNESW_VERSION"), deliveryMethod=args.delivery_method, workflowMethod=args.workflow_method,
        processID = args.processID, processHASH = args.processHASH, projectID=args.projectID, sam_web_uri = args.sam_web_uri,
        debug = args.debug, formatString=args.formatString)
        returncode = lar.DoLAr(0, args.processID)

        sys.exit(returncode)

## command line, explains the variables.
if __name__ == "__main__":
    main()
