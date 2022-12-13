import os,sys
import samweb_client
import LArWrapper
samweb = samweb_client.SAMWebClient(experiment='dune')

TEST = True
def testProject(defname="schellma-run5141-PDSPProd4", appFamily="samtest", appName="test", appVersion=None, fcl="./eventdump.fcl",method="samweb"):
    appVersion = os.environ["DUNESW_VERSION"]
    projectname = samweb.makeProjectName(defname)
    projectinfo = samweb.startProject(projectname, defname)
    print (projectinfo)
    projecturl = projectinfo["projectURL"]
    print ("Project name is %s" % projectinfo["project"])
    print ("Project URL is %s" % projecturl)
    info = samweb.projectSummary(projecturl)
    print (info)
    projectID = info["project_id"]
    deliveryLocation = None # set this to a specific hostname if you want - default is the local hostname

    cpid = samweb.startProcess(projecturl, appFamily, appName, appVersion, deliveryLocation)
    print ("Consumer process id %s" % cpid)
    processurl = samweb.makeProcessUrl(projecturl, cpid)
    try:
        larW = LArWrapper.LArWrapper(fcl=fcl,appFamily=appFamily,appName=appName,\
        appVersion=appVersion,deliveryMethod="samweb",workflowMethod="interactive",\
        sam_web_uri=projecturl,processID=cpid,projectID=projectID,\
        dataTier="out1:sam-user",dataStream="out1:test",n=120)
        retcode = larW.DoLAr(0,0)
        list = larW.LArResults()
        print ("return code",retcode)
    except:
        print ("LArWrapper failed, clean up")

#    while True:
#        try:
#            newfile = samweb.getNextFile(processurl)['url']
#            print "Got file %s" % newfile
#        except samweb_client.NoMoreFiles:
#            print "No more files available"
#            break
#
#        samweb.releaseFile(processurl, newfile)
#        print "Released file %s" % newfile

    samweb.stopProject(projecturl)
    print (samweb.projectSummaryText(projecturl))
    print ("Project ended")

if __name__ == '__main__':

    testProject()
