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
    def __init__(self,fcl=None,replicas=None,flist="",n=None,nskip=None,o=None,appFamily=None, appName=None, appVersion=None, deliveryMethod=None, workflowMethod=None, projectID=None, formatString="runLar_%s_%%tc_%s_%s_reco.root"):
        self.fcl = fcl
        self.flist = flist
        self.n = n
        self.nskip = nskip
        self.o = os
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
    
    def DoLAr(self,cluster=0,process=0):
        stamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S%Z")
        fname = self.formatString%(self.projectID, cluster, process)
        self.oname = fname.replace(".root",".out").replace("%tc",stamp)
        self.ename = fname.replace(".root",".err").replace("%tc",stamp)
        ofile = open(self.oname,'w')
        efile = open(self.ename,'w')
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
        
        info = {"application_family":self.appFamily,"application_name":self.appName, "application_version":self.appVersion,"delivery_method":self.deliveryMethod,"workflow_method":self.workflowMethod}
        
        if self.deliveryMethod == "dd":
            
            info["dd_worker_id"]=os.environ["MYWORKERID"]
            info["project_id"]=self.projectID
            unused_replicas = logparse.addreplicainfo(self.replicas)
            unused_files = []
            for u in unused_replicas:
                unused_files.append(u["namespace"]+":"+u["name"])
            logparse.addmetacatinfo()
            print ("files not used",unused_files)
        elif deliverMethod == "samweb":
            unused_files = logparse.findmissingfiles(self.files)
            logparse.addsaminfo()
            
        
        logparse.addinfo(info)
        logparse.addsysinfo()

        # write out json files for processed files whether closed properly or not.  Those never opened don't get logged.
        logparse.writeme()
        return unused_files

  
