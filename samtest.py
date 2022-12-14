import os,sys
import samweb_client
import LArWrapper
from argparse import ArgumentParser as ap

samweb = samweb_client.SAMWebClient(experiment='dune')

TEST = True
def testProject(defname="schellma-run5141-PDSPProd4", appFamily="samtest", appName="test", appVersion=None, fcl="eventdump.fcl",method="samweb",n=-1,nskip=0,dataTier="out1:sam-user",dataStream="out1:test"):
    appVersion = os.environ["DUNESW_VERSION"]
    projectname = samweb.makeProjectName(defname)
    projectinfo = samweb.startProject(projectname, defname)
    #print (projectinfo)
    projecturl = projectinfo["projectURL"]
    print ("Project name is %s" % projectinfo["project"])
    print ("Project URL is %s" % projecturl)
    info = samweb.projectSummary(projecturl)
    print (info)
    projectID = info["project_id"]
    print ("Project ID is %s" % projectID)
    deliveryLocation = None # set this to a specific hostname if you want - default is the local hostname

    cpid = samweb.startProcess(projecturl, appFamily, appName, appVersion, deliveryLocation)
    print ("Consumer process id %s" % cpid)
    processurl = samweb.makeProcessUrl(projecturl, cpid)
    try:
        larW = LArWrapper.LArWrapper(fcl=fcl,appFamily=appFamily,appName=appName,\
        appVersion=appVersion,deliveryMethod="samweb",workflowMethod="interactive",\
        sam_web_uri=projecturl,processID=cpid,projectID=projectID,\
        dataTier=dataTier,dataStream=dataStream,n=n,nskip=nskip)
        print ("before DoLAr")
        retcode = larW.DoLAr(0,0)
        list = larW.LArResults()
        print ("return code",retcode)
    except:
        print ("LArWrapper failed, clean up")
        retcode = 99

    samweb.stopProject(projecturl)
    print (samweb.projectSummaryText(projecturl))
    print ("Project ended")
    print ("LArWrapper returned",retcode)
    return retcode

if __name__ == '__main__':
    parser = ap()
    parser.add_argument('--defName', default="schellma-run5141-PDSPProd4", type=str, help='samweb dataset definition name')
    parser.add_argument('--appFamily',default='test', type=str, help='samweb needs this')
    parser.add_argument('--appName', default='test',type=str, help='samweb needs this')
    parser.add_argument('--appVersion', default=os.getenv('DUNESW_VERSION'), type=str, help='samweb needs this')
    parser.add_argument('--dataTier', default='out1:sam-user',type=str, help='data tier for output file')
    parser.add_argument('--dataStream', default='out1:test',type=str, help='data stream for output file')
    parser.add_argument('-o', default="temp.root", type=str, help='output event stream file')
    parser.add_argument('-c', required=True, type=str, help='name of fcl file')
    parser.add_argument('--user', default = os.getenv("USER"),type=str, help='user name')
    parser.add_argument('-n', type=int, default=150, help='number of events total to process')
    parser.add_argument('--nskip', type=int, default=0, help='number of events to skip before starting')
    # if restarting need these
 #   parser.add_argument('--projectID', type=int, default=0, help='integer that identifies the project, samweb/dd/wfs')
  #  parser.add_argument('--sam_web_uri',type=str, help='samweb url for the project')
   
    args = parser.parse_args()

    testProject(defname=args.defName, appFamily=args.appFamily, appName=args.appName, appVersion=args.appVersion, fcl=args.c,method="samweb",n=args.n,nskip=args.nskip,dataTier=args.dataTier,dataStream=args.dataStream)