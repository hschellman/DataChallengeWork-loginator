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
if "SAM_EXPERIMENT" in os.environ:
    import samweb_client


from metacat.webapi import MetaCatClient
import string,datetime#,dateutil
from datetime import date,timezone,datetime
#from dateutil import parser


DEBUG=False

class Loginator:

    def __init__(self,logname):
        if not os.path.exists(logname):
            print ("no such file exists, quitting",logname)
            sys.exit(1)
        self.logname = logname
        self.logfile = open(logname,'r')
        self.outobject ={}
        self.info = self.getsysinfo()
        self.tags = ["Opened input file", "Closed input file","VmHWM"]
        self.template = {
            "source_rse":None,  #
            "user":None,  # (who's request is this)
            "job_id":None, # (jobsubXXX03@fnal.gov)
            "timestamp_for_start":None,  #
            "timestamp_for_end":None,  #
            "duration":None,  # (difference between end and start)
            "file_size":None,  #
            "application_family":None,  #
            "application_name":None,  #
            "application_version":None,  #
            "final_state":None,  # (what happened?)
            "project_name":None, #(wkf request_id?)"
            "file_name":None,  # (not including the metacat namespace)
            "fid":None, # metacat fid
            "data_tier":None,  # (from metacat)
            "data_stream":None,
            "run_type":None,
            "job_node":None,  # (name within the site)
            "job_site":None,  # (name of the site)
            "country":None,  # (nationality of the site)
            "campaign":None,  # (DUNE campaign)
            "delivery_method":None, #(stream/copy)
            "workflow_method":None,
            "access_method":None, #(samweb/dd)
            "path":None,
            "namespace":None,
            "real_memory":None,
            "project_id":None,
            "delivery_method":None
        }

## return the first tag or None in a line
    def findme(self,line):
        for tag in self.tags:
            if tag in line:
                if DEBUG: print (tag,line)
                return tag
        return None

## get system info for the full job
    def getsysinfo(self):
        info = {}
        # get a bunch of system thingies.
        if os.getenv("GRID_USER") != None:
            info["user"]=os.getenv("GRID_USER")
        else:
            info["user"]=os.getenv("USER")
        info["job_node"] = os.getenv("HOST")
        info["job_site"] = os.getenv("GLIDEIN_DUNESite")
        #info["POMSINFO"] = os.getenv("poms_data")  # need to parse this further
        return info

## read in the log file and parse it, add the info
    def readme(self):
        object = {}
        memdata = None
        for line in self.logfile:
            tag = self.findme(line)
            if DEBUG: print (tag,line)
            if tag == None:
                continue
            if "VmHWM" == tag:
                memdata = line.split("VmHWM = ")[1].strip()
            if "file" in tag:
                data = line.split(tag)
                filefull = data[1].strip().replace('"','')
                timestamp = data[0].strip()
                filename = os.path.basename(filefull).strip()
                filepath = os.path.dirname(filefull).strip()
                dups = 0
                if "Opened" in tag:
                    localobject = {}
                    if DEBUG: print ("filename was",filename,line)

                    localobject = self.template.copy()
                    localobject["timestamp_for_start"] = timestamp
                    start = timestamp
                    localobject["path"]=filepath
                    localobject["file_name"] = filename
                    if DEBUG: print ("filepath",filepath)
                    if "root" in filepath[0:10]:
                        if DEBUG: print ("I am root")
                        tmp = filepath.split("//")
                        localobject["source_rse"] = tmp[1]
                        localobject["deliver_method"] = "xroot"
                    for thing in self.info:
                        localobject[thing] = self.info[thing]
                    localobject["final_state"] = "Opened"
                if "Closed" in tag:
                    localobject["timestamp_for_end"] = timestamp
                    localobject["duration"]=self.duration(start,timestamp)
                    localobject["final_state"] = "Closed"
                object[filename] = localobject
                continue

                #print ("mem",memdata,filename)
        # add the memory info if available
        for thing in object:
            if memdata != None: object[thing]["real_memory"]=memdata
            #print ("mem",object[thing]["real_memory"])
        self.outobject=object

    def addinfo(self,info):
        for s in info:
            if s in self.outobject:
                print ("Loginator replacing",s, self.outobject[s],self.info[s])
            else:
                for f in self.outobject:
                    self.outobject[f][s] = info[s]
                    if DEBUG: print ("adding",s,info[s])

    def addsaminfo(self):
        samweb = samweb_client.SAMWebClient(experiment='dune')
        for f in self.outobject:
            if DEBUG: print ("f ",f)
            meta = samweb.getMetadata(f)
            self.outobject[f]["namespace"]="samweb"
            self.outobject[f]["access_method"]="samweb"
            for item in ["data_tier","file_type","data_stream","group","file_size"]:
                self.outobject[f][item]=meta[item]
            for run in meta["runs"]:
                self.outobject[f]["run_type"] = run[2]
                break

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
            print (f,namespace)
            meta = mc_client.get_file(name=f,namespace=namespace)
            print ("metacat answer",f,namespace)
            if meta == None:
                print ("no metadata for",f)
                continue
            self.outobject[f]["access_method"]="metacat"
            for item in ["data_tier","file_type","data_stream","run_type"]:
                if "core."+item in meta["metadata"].keys():
                    self.outobject[f][item]=meta["metadata"]["core."+item]
                else:
                    print ("no", item, "in ",list(meta["metadata"].keys()))
            self.outobject[f]["file_size"]=meta["size"]
            self.outobject[f]["fid"]=meta["fid"]
            self.outobject[f]["namespace"]=namespace

    def addreplicainfo(self,replicas,test=False):
        notfound = []
        for f in self.outobject:
            self.outobject[f]["rse"] = None

        for r in replicas:
            found = False
            for f in self.outobject:
                if f == r["name"]:
                    print ("replica match",r)
                    found = True
                    if "rse" in r:
                        self.outobject[f]["rse"] = r["rse"]
                    if "namespace" in r:
                        self.outobject[f]["namespace"] = r["namespace"]
                print (self.outobject[f])
            if not found:
                notfound.append(r)

        return notfound


#    def metacatinfo(self,namespace,filename):
#        print ("do something here")


    def writeme(self):
        result = []
        for thing in self.outobject:
            outname = thing+".process.json"
            outfile = open(outname,'w')
            json.dump(self.outobject[thing],outfile,indent=4)
            outfile.close()
            result.append(outname)
        return result

    def human2number(self,stamp):
        #15-Nov-2022 17:24:41 CST https://docs.python.org/3/library/time.html#time.strftime
        format = "%d-%b-%Y %H:%M:%S"
        # python no longer accepts time zones.  We only want the different but need to correct for DT
        thetime  = datetime.strptime(stamp[:-4],format)
        epoch = datetime.utcfromtimestamp(0)
        if "DT" in stamp:
            stamp += 3600
        return (thetime-epoch).total_seconds()

    def duration(self,start,end):
        t0 = self.human2number(start)
        t1 = self.human2number(end)
        return t1-t0

def envScraper():
    env = os.environ
    if "apple" in env["CLANGXX"]:
        f = open("bigenv.txt")
        env = {}
        for a in f.readlines():
            line = a.split("=")
            env[line[0]] = line[1]
    digest = {}
    for k in env.keys():
        if "SETUP_" in k:
            it = env[k].split(" ")
            digest[k] = {"Product":it[0],"Version":it[1]}
    return digest


def test():
    parse = Loginator(sys.argv[1])
    print ("looking at",sys.argv[1])
    parse.readme()
    parse.addinfo(parse.getsysinfo())
   # parse.addsaminfo()
    parse.addreplicainfo([])
    parse.addmetacatinfo("dc4-hd-protodune") # argument is there for testing when you don't have replica list.
    parse.writeme()


if __name__ == '__main__':
    test()