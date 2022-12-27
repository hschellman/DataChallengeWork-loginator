"""! @brief Art logfile parser """
##
# @mainpage Loginator.py
#
# @section description_main Description
# A program for parsing art logs to put information into DUNE job monitoring.
#
#
# Copyright (c) 2022 Heidi Schellman, Oregon State University
##
# @file Loginator.py

import string,time,datetime,json,os,sys

## try to set up samweb if available
if "SAM_EXPERIMENT" in os.environ:
    import samweb_client


from metacat.webapi import MetaCatClient
import string,datetime
from datetime import date,timezone,datetime
 

DEBUG=False

##  Loginator class should not care what access method you are using

class Loginator:

## initialization, requires an Art logfile to parse and creates a template json file

    def __init__(self,logname):
        if not os.path.exists(logname):
            print ("no such file exists, quitting",logname)
            sys.exit(1)
        self.logname = logname
        self.logfile = open(logname,'r')
        self.outobject ={}
        self.info = self.getsysinfo()
        self.tags = ["Opened input file", "Closed input file","MemReport  VmPeak","CPU","Events total","Initiating request to open input file"]
        self.template = {
             # job attributes
            "user":None,  # (who's request is this)
            "job_id":None, # (jobsubXXX03@fnal.gov)
            "job_node":None,  # (name within the site)
            "job_site":None,  # (name of the site)
            "country":None,  # (nationality of the site)
            "job_real_memory":None,
            "job_wall_time":None,
            "job_cpu_time":None,
            "job_total_events":None,
            # processing attributes
            "project_id":0,
            "delivery_method":None, #(samweb/dd/wfs)
            "workflow_method":None,
            "access_method":None, #(stream/copy)
            "timestamp_for_request":None,
            "timestamp_for_start":None,  #
            "timestamp_for_end":None,  #
            "application_family":None,  #
            "application_name":None,  #
            "application_version":None,  #
            "final_state":None,  # (what happened?)
            "project_name":None, #(wkf request_id?)"
            "duration":None,  # (difference between end and start)
            "path":None,
            "rse":None,
            # file attributes from metacat
            "file_size":None,  #
            "file_type":None,  #
            "file_name":None,  # (not including the metacat namespace)
            "fid":None, # metacat fid
            "data_tier":None,  # (from metacat)
            "data_stream":None,
            "run_type":None,
            "file_format":None,
            "file_campaign":None,  # (DUNE campaign)
            "namespace":None,
            "event_count":None
        }

    ## utility to print the environment
    def envPrinter(self):
        env = os.environ
        for k in env:
            print ("env ",k,"=",env[k])


## look at a line of text and find the first tag from self.tags in the line
    def findme(self,line):
        for tag in self.tags:
            if tag in line:
                if DEBUG: print (tag,line)
                return tag
        return None

## safe access for a dictionary element, returns None rather than failing
    def getSafe(self,dict,envname):
        if envname in dict:
            if DEBUG: print ("found ",envname)
            return dict[envname]
        else:
            return None


## get system info for the running process, shared with all files processed
    def getsysinfo(self):
        info = {}
        # get a bunch of system thingies.
        if os.getenv("GRID_USER") != None:
            info["user"]=os.getenv("GRID_USER")
        else:
            info["user"]=os.getenv("USER")
        info["job_id"] = self.getSafe(os.environ,"JOBSUBJOBID")
        info["job_node"] = self.getSafe(os.environ,"NODE_NAME")
        #info["job_node"] = os.getenv("HOST")
        info["job_site"] = os.getenv("GLIDEIN_DUNESite")
        #info["POMSINFO"] = os.getenv("poms_data")  # need to parse this further
        return info

## add that system info to the record
    def addsysinfo(self):
        self.addinfo(self.getsysinfo())

## read in the log file and parse it, add the info
# lines with specific tags get logged as file or end of job activities
    def readme(self):
        object = {}
        memdata = None
        cpudata = None
        walldata = None
        totalevents = None
         
        for line in self.logfile:
            tag = self.findme(line)
            if DEBUG: print (tag,line)
            
            # not relevant, skip
            if tag == None:
                continue
            
            # memory from end of job
            if "MemReport  VmPeak" == tag:
                memdata = line.split("VmHWM = ")[1].strip()
                
            # cpu/walltime from end of job
            if "CPU" == tag:
                timeline = line.strip().split(" ")
                if len(timeline) < 7:
                    continue
                cpudata = timeline[3]
                walldata = timeline[6]
           
            # total events processedf from end of job
            if "Events total" in tag:
                eventline  = line.strip().split(" ")
                if len(eventline) < 11:
                    continue
                totalevents = eventline[4]
                
            # file request/open/close activities
            if "file" in tag:
                data = line.split(tag)
                filefull = data[1].strip().replace('"','')
                timestamp = data[0].strip()
                filename = os.path.basename(filefull).strip()
                filepath = os.path.dirname(filefull).strip()
                dups = 0
                
                # start - ask for file
                if "Initiating" in tag:
                    localobject = self.template.copy()
                    localobject["final_state"] = "Started"
                    localobject["timestamp_for_request"] = timestamp
                    localobject["final_state"] = "Requested"
                    localobject["path"]=filepath
                    localobject["file_name"] = filename
                    if "root" in filepath[0:10]:
                        if DEBUG: print ("I am root")
                        tmp = filepath.split("//")
                        localobject["rse"] = tmp[1]
                        localobject["access_method"] = "xroot"
                    for thing in self.info:
                        localobject[thing] = self.info[thing]
                # open the file
                if "Opened" in tag:
                    
                    localobject = {}
                    if DEBUG: print ("filename was",filename,line)
                    localobject["timestamp_for_start"] = timestamp
                    start = timestamp
                    localobject["path"]=filepath
                    localobject["file_name"] = filename
                    if DEBUG: print ("filepath",filepath)
                    if "root" in filepath[0:10]:
                        if DEBUG: print ("I am root")
                        tmp = filepath.split("//")
                        localobject["rse"] = tmp[1]
                        localobject["access_method"] = "xroot"
                    for thing in self.info:
                        localobject[thing] = self.info[thing]
                    localobject["final_state"] = "Opened"
                    
                # Close the file
                if "Closed" in tag:
                    localobject["timestamp_for_end"] = timestamp
                    localobject["duration"]=self.duration(start,timestamp)
                    localobject["final_state"] = "Closed"
                object[filename] = localobject
                continue

                
        # add the job info to all file records if available
        for thing in object:
            if memdata != None: object[thing]["job_real_memory"]=memdata
            if walldata != None: object[thing]["job_wall_time"]=walldata
            if cpudata != None: object[thing]["job_cpu_time"]=cpudata
            if totalevents != None: object[thing]["job_total_events"]=totalevents
            #print ("mem",object[thing]["real_memory"])
        self.outobject=object

## add information from a dictionary to the record
    def addinfo(self,info):
        for s in info:
            if s in self.outobject:
                print ("Loginator.addinfo replacing",s, self.outobject[s],self.info[s])
            else:
                for f in self.outobject:
                    self.outobject[f][s] = info[s]
                    if DEBUG: print ("adding",s,info[s])

## add input file information from sam to the record
    def addsaminfo(self):
        if "SAM_EXPERIMENT" in os.environ:
            import samweb_client
        else:
            print ("You need to set up samweb to get sam info")
            sys.exit(0)
        samweb = samweb_client.SAMWebClient(experiment='dune')
        for f in self.outobject:
            if DEBUG: print ("f ",f)
            meta = samweb.getMetadata(f)
            self.outobject[f]["namespace"]="samweb"
            self.outobject[f]["delivery_method"]="samweb"
            for item in ["event_count","data_tier","file_type","data_stream","file_size","file_format"]:
                self.outobject[f][item]=meta[item]
            if "DUNE.campaign" in meta:
                self.outobject[f]["file_campaign"] = meta["DUNE.campaign"]
            for run in meta["runs"]:
                self.outobject[f]["run_type"] = run[2]
                break

## add file information from metacat to the record
    def addmetacatinfo(self,defaultNamespace=None):
        os.environ["METACAT_SERVER_URL"]="https://metacat.fnal.gov:9443/dune_meta_demo/app"
        mc_client = MetaCatClient('https://metacat.fnal.gov:9443/dune_meta_demo/app')
        for f in self.outobject:
            if "namespace" in self.outobject[f] and self.outobject[f]["namespace"] != None:
                namespace = self.outobject[f]["namespace"]
            else:
                print ("set default namespace for file",f,defaultNamespace)
                namespace = defaultNamespace
                if namespace == None:
                    print (" could not set namespace for",f)
                    continue
            if DEBUG: print (f,namespace)
            meta = mc_client.get_file(name=f,namespace=namespace)
            if DEBUG: print ("metacat answer",f,namespace)
            if meta == None:
                print ("no metadata for",f)
                continue
            self.outobject[f]["delivery_method"]="dd"
            for item in ["data_tier","file_type","data_stream","run_type","event_count","file_format"]:
                if "core."+item in meta["metadata"].keys():
                    self.outobject[f][item]=meta["metadata"]["core."+item]
                else:
                    print ("addmetacatinfo: no", item, "in ",list(meta["metadata"].keys()))
            self.outobject[f]["file_size"]=meta["size"]
            if "DUNE.campaign" in meta["metadata"]:
                self.outobject[f]["file_campaign"]=meta["metadata"]["DUNE.campaign"]
            self.outobject[f]["fid"]=meta["fid"]
            self.outobject[f]["namespace"]=namespace

## get some details from the data dispatcher on file locations
    def addreplicainfo(self,replicas,test=False):
        notfound = []
        for f in self.outobject:
            self.outobject[f]["rse"] = None

        for r in replicas:
            found = False
            for f in self.outobject:
                if f == r["name"]:
                    if DEBUG: print ("replica match",r)
                    found = True
                    if "rse" in r:
                        self.outobject[f]["rse"] = r["rse"]
                    if "namespace" in r:
                        self.outobject[f]["namespace"] = r["namespace"]
                if DEBUG: print (self.outobject[f])
            if not found:
                print (r,"appears in replicas but not in Lar Log, need to mark as unused")
                notfound.append(r)

        return notfound

## add information from data dispatcher on files reserved for this process to the record.
# can use this information to mark unused files
    def findmissingfiles(self,files):
        notfound = []

        for r in files:
            found = False
            if ":" in r:
                s = r.split(":")
                name = s[1]
                namespace = s[0]
            else:
                name = r
                namespace = "samweb"

            for f in self.outobject:
                if f == name:
                    if DEBUG: print ("file match",r)
                    found = True
                    self.outobject[f]["namespace"] = namespace
            if not found:
                print (r,"appears in replicas but not in Lar Log, need to mark as unused")
                notfound.append(r)

        return notfound




## dump info to json file
    def writeme(self):
        result = []
        for thing in self.outobject:
            outname = "%s_%d_process.json" %(thing,self.outobject[thing]["project_id"])
            outfile = open(outname,'w')
            json.dump(self.outobject[thing],outfile,indent=4)
            outfile.close()
            result.append(outname)
        return result

## utility to parse timestamps
    def human2number(self,stamp):
        #15-Nov-2022 17:24:41 CST https://docs.python.org/3/library/time.html#time.strftime
        format = "%d-%b-%Y %H:%M:%S"
        # python no longer accepts time zones.  We only want the different but need to correct for DT
        #print ("human2number converting",stamp)
        thetime  = datetime.strptime(stamp[0:19],format)
        epoch = datetime.utcfromtimestamp(0)
        if "DT" in stamp:
            stamp += 3600
        return (thetime-epoch).total_seconds()

## utility to change timestamps into time delta
    def duration(self,start,end):
        t0 = self.human2number(start)
        t1 = self.human2number(end)
        return t1-t0


## test this
def test():
    parse = Loginator(sys.argv[1])
    #print ("looking at",sys.argv[1])
    parse.readme()
    parse.addsysinfo()
    parse.addreplicainfo([])
    if "SAM_EXPERIMENT" in os.environ:
        parse.addsaminfo()
    else:
        parse.addmetacatinfo("pdsp_det_reco")
            #parse.addmetacatinfo("dc4-hd-protodune") # argument is there for testing when you don't have replica list.
    parse.writeme()


if __name__ == '__main__':
    test()