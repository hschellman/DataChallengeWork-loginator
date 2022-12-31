from argparse import ArgumentParser as ap
import sys
import os
import subprocess
import time
import datetime
import requests
import submit_dd_jobs

''' Data Dispatcher interface
Mainly written by Jacob Calcutt, 2022
Mods for logging outputs by H. Schellman, 2022
'''

# makes it easier to build docs on mac without invoking the real code
if not "HOST" in os.environ or not "apple" in os.environ["HOST"]:

    from data_dispatcher.api import DataDispatcherClient
    from data_dispatcher.api import APIError

import LArWrapper
import Loginator


''' utility codes '''

# make a string out of none for formatted Printing
def NoneToString(thing):
    if thing == None:
        return "-"
    else:
        return thing

def makedid(namespace,name):
    return "%s:%s"%(namespace,name)

def call_and_retry(func):
  def inner1(*args, **kwargs):
    nretries = 0
    while nretries < 5:
      try:
        print("call_and_retry",datetime.datetime.now())
        func(*args, **kwargs)
        break
      except (requests.exceptions.ConnectionError, APIError) as err:
        print('Caught', err.args)
        print(f'Will wait {args[0].retry_time} seconds and try again')
        time.sleep(args[0].retry_time)
        nretries += 1
    if nretries > 4:
      print("call_and_retry",'Too many retries')
      sys.exit(1)
  return inner1

def call_and_retry_return(func):
  def inner1(*args, **kwargs):
    nretries = 0
    while nretries < 5:
      try:
        print("call_and_retry_return",datetime.datetime.now())
        result = func(*args, **kwargs)
        break
      except (requests.exceptions.ConnectionError, APIError) as err:
        print('Caught', err.args)
        print(f'Will wait {args[0].retry_time} and try again')
        time.sleep(args[0].retry_time)
        nretries += 1
    if nretries > 4:
      print("call_and_retry_return",'Too many retries')
      sys.exit(1)
    return result
  return inner1



class DDInterface:
  ''' Interface to the Data Dispatcher
  '''
  def __init__(self, debug=False, dataset=None, namespace=None, lar_limit=0, timeout=120, wait_time=60, wait_limit=5,\
   appFamily=None, appName=None, appVersion=None, workflowMethod="dd"):
    '''
    Main interface for the data Dispatcher

    :param debug: add printout and send stdout to console instead of a files
    :type debug: bool

    :param dataset: data dispatcher dataset did
    :type dataset: stream

    :param lar_limit: maximum number of files to deliver
    :type lar_limit: int

    :param appFamily: Optional Information about process
    :type appFamily: str

    :param appVersion: Optional Information about process
    :type appVersion: str

    :param appName: Optional Information about process
    :type appName: str

    :return: the lar return code
    :rtype: int
    '''

    self.dataset = dataset
    self.limit = 1#limit
    self.namespace = namespace

    ''' namespace is an extra qualifier used in tests - deprecated '''
    if namespace == None:
        self.query = '''files from %s limit %i'''%(self.dataset, self.limit)
    else:
        query_args = (self.dataset, self.namespace, self.limit)
        self.query = '''files from %s where namespace="%s" limit %i'''%query_args  # this is not a good idea
    self.debug = debug
    if self.debug: print ("DDInterface: the query is:",self.query)
    self.worker_timeout = 3600*5
    self.lar_limit = lar_limit
    self.proj_id = -1
    self.proj_exists = False
    self.proj_state = None
    self.loaded_files = [] # list of files with all the replicas for worker to choose from
    self.input_replicas = []  # list of the replicas actually sent to Lar
    self.loaded_file_uris = []
    self.loaded = False
    self.dd_timeout = timeout
    self.wait_time = wait_time
    self.max_wait_attempts = wait_limit
    self.hit_timeout = False
    self.lar_return = -1
    self.lar_file_list = ''
    self.n_waited = 0
    self.next_failed = False
    self.next_replicas = []
    self.appFamily = appFamily
    self.appName = appName
    self.appVersion = appVersion
    self.deliveryMethod="dd"
    self.workflowMethod=workflowMethod
    self.unused_files = []

    self.retry_time = wait_time


    #try:
    #  from data_dispatcher.api import DataDispatcherClient
    #  from data_dispatcher.api import APIError
    #  print('Loaded DataDispatcher')
    #except:
    #  print('Failed DataDispatcher')
    #  pass
    self.dd_client = DataDispatcherClient()

  @call_and_retry
  def Login(self, username):

    """
    Get credentials for DD and metacat
    :param username: username (normally fnal service account)
    :type username: str
    """
    print("Try to login:", datetime.datetime.now())
    self.dd_client.login_x509(username, os.environ['X509_USER_PROXY'])
    print("Login:", datetime.datetime.now())


  def SetLarLimit(self, limit):
    self.lar_limit = limit

  def CreateProject(self):
    """
    Start a data dispatcher project
    """
    query_files = mc_client.query(self.query)
    proj_dict = self.dd_client.create_project(
        query_files, query=self.query, worker_timeout=self.worker_timeout)
    print("CreateProject",datetime.datetime.now())
    self.proj_state = proj_dict['state']
    self.proj_id = proj_dict['project_id']
    self.proj_exists = True
    #print(proj_dict)

  def PrintFiles(self):
    print('Printing files')
    for j in self.loaded_files:
      print(j['name'])

  @call_and_retry
  def next_file(self):
    """
    Ask the project for a new file to process
    """
    print ("ask for next file")
    if "GLIDEIN_DUNESite" in os.environ:
        site = os.getenv("GLIDEIN_DUNESite")
    elif "HOSTNAME" in os.environ:
        site = os.getenv("HOSTNAME")
    else:
        site = None
    print ('looking for files near',site)
    # want to add cpu_site=site
    self.next_output = self.dd_client.next_file(
        self.proj_id, timeout=self.dd_timeout,
        worker_id=os.environ['MYWORKERID'])
    print("next_file ",self.next_output,datetime.datetime.now())

  @call_and_retry
  def file_done(self, did):

      """
      Mark file as done
      :param did: did <namespace>:<filename> of finished file
      :type did: str
      """
      print ("try to mark file done", datetime.datetime.now())
      self.dd_client.file_done(self.proj_id, did)
      print("file_done ", datetime.datetime.now())

  @call_and_retry
  def file_failed(self, did, do_retry=True):
    print ("try to mark file failed", datetime.datetime.now())
    self.dd_client.file_failed(
        self.proj_id, did,
        #'%s:%s'%(self.next_output['namespace'], self.next_output['name']),
        retry=do_retry)
    print("file_failed ", datetime.datetime.now())

  @call_and_retry_return
  def get_project(self, proj_id):
    print ("try to get project", datetime.datetime.now())
    proj = self.dd_client.get_project(proj_id, with_files=False)
    print (proj)
    print("get_project", datetime.datetime.now())
    return proj


  def dump_project(self, proj_id):
    proj = self.dd_client.get_project(proj_id, with_files=True)
    print ("dumping project",proj_id)
    for k in proj:
        if k == "file_handles":
            continue
        print (k,proj[k])
    for f in proj["file_handles"]:
        reserved = f["reserved_since"]
        if reserved == None:
            reserved = "-"
        else:
            t = datetime.datetime.fromtimestamp(reserved,tz=datetime.timezone.utc)
            reserved = t.isoformat()[0:19] + 'Z'

        print("%10s\t%d\t%21s\t%8s\t%s:%s"%(f["state"],f["attempts"],(reserved),NoneToString(f["worker_id"]),f["namespace"],f["name"]))


  def LoadFiles(self):
    count = 0
    ##Should we take out the proj_state clause?
    while (count < self.lar_limit and not self.next_failed): #and
           #self.proj_state == 'active'):
      print('Loadfiles: Attempting fetch %i/%i'%(count, self.lar_limit), self.next_failed)
      self.Next()
      if self.next_output == None:
        ## this shouldn't happen, but if it does just exit the loop
        print ("next_output = None")
        break
      elif self.next_output == True:
        ##this means the fetch timed out.

        ##First --> check that there are files reserved by other jobs.
        ##          If there aren't, just exit the loop and try processing
        ##          any files (if any) we have
        #file_handles = self.dd_client.get_project(self.proj_id)['file_handles']
        #total_reserved = sum([fh['state'] == 'reserved' for fh in file_handles])
        #if total_reserved == count:
        #  print('Equal number of reserved and loaded files. Ending loop')
        #  break
        #elif total_reserved < count:
        #  ##This shouldn't happen... If it does, fail and make some noise
        #  sys.stderr.write("Something bad happened. N reserved in project < n reserved in this job: (%i/%i) \n"%(total_reserved, count))
        #  sys.exit(1)
        if count > 0:
          print('data dispatcher next_file timed out. This job has at least one file reserved. Will continue.')
          break
        else:
          ##We know we have externally-reserved files.
          ##try waiting a user-defined amount of time
          ##for a maximum number of attempts
          ##-- if at max, go on to loop
          if self.n_waited < self.max_wait_attempts:
            print("data dispatcher next_file timed out. Waiting %i seconds before attempting again"%self.wait_time)
            print("Wait attempts: %i/%i"%(self.n_waited, self.max_wait_attempts))
            time.sleep(self.wait_time)
            self.n_waited += 1
          else:
            print("Hit max wait limit. Ending attempts to load files")
            break
      elif self.next_output == False:
        ##this means the project is done -- just exit the loop.
        print("Project is done -- exiting file fetch loop")
        break
      else:
        ##we successfully got a file (at least nominally). Check that it has replicas available.
        ##If it doesn't, compromise it to a permament end
        print ("got a file",datetime.datetime.now())
        if len(self.next_replicas) > 0:
          self.loaded_files.append(self.next_output)
          count += 1
          ##also reset the number of times waited
          self.n_waited = 0
        else:
          print('Empty replicas -- marking as failed',self.next_output)
          self.file_failed(
              '%s:%s'%(self.next_output['namespace'], self.next_output['name']),
              do_retry=False)
    self.loaded = True
    print("Loaded %i files. Moving on."%len(self.loaded_files))
    self.PrintFiles()

  def Next(self):
    if self.proj_id < 0:
      raise ValueError('DDInterface::Next -- Project ID is %i. Has a project been created?'%self.proj_id)
    ## exists, state, etc. -- TODO
    self.next_file()
    if self.next_output == None:
      self.next_failed = True
      return

    if type(self.next_output) == dict:
      self.next_replicas = list(self.next_output['replicas'].values())


  def MarkFiles(self, failed=False,badlist=[]):
    state = 'failed' if failed else 'done'
    #nretries = 0

    for j in self.loaded_files:
      did = makedid(j['namespace'], j['name'])
      # mark all as failed if that is how things are set
      if failed:
        print('Marking failed')
        self.file_failed(did)
      # ok, maybe some failed,
      else:
        good = True
        for b in badlist:
            if did == b:
                print ("this file was not used, mark as failed",did)
                good = False
                break
        if good:
            print('Marking done')
            self.file_done(did)
            print('Marked as done')
        else:
            print('Marking failed')
            self.file_failed(did)
            print('Marked as failed')

  def SaveFileDIDs(self):
    lines = []
    for i in range(len(self.loaded_files)):
      f = self.loaded_files[i]
      if i < len(self.loaded_files)-1:
        lines.append('{"did":"%s:%s"},\n'%(f['namespace'], f['name']))
      else:
        lines.append('{"did":"%s:%s"}\n'%(f['namespace'], f['name']))

    with open('loaded_files.txt', 'w') as f:
      f.writelines(lines)

  def SetWorkerID(self):
    os.environ['MYWORKERID'] = self.dd_client.new_worker_id()

  def AttachProject(self, proj_id):
    self.proj_id = proj_id
    #proj = self.dd_client.get_project(proj_id, with_files=False)
    #proj = self.get_project(proj_id)
    #print(proj)
    #if proj == None:
    #  self.proj_exists = False
    #else:
    #  self.proj_exists = True
    #  self.proj_state = proj['state']

  def BuildFileListString(self):
    for j in self.loaded_files:
      replicas = list(j['replicas'].values())
      print ("possible replicas",replicas)
      if len(replicas) > 0:
        #Get the last replica
        #replica = replicas[len(replicas)-1]
        replica = replicas[0] # go with first.
        self.input_replicas.append(replica)
        print('Replica:', replica)
        uri = replica['url']
        if 'https://eospublic.cern.ch/e' in uri: uri = uri.replace('https://eospublic.cern.ch/e', 'xroot://eospublic.cern.ch//e')
        self.lar_file_list += uri
        self.lar_file_list += ' '
      else:
        print('Empty replicas -- marking as failed')

        ##TODO -- pop entry
        self.dd_client.file_failed(self.proj_id, '%s:%s'%(j['namespace'], j['name']))
        print(datetime.datetime.now())

  def RunLAr(self, fcl=None, n=-1, nskip=0):
    print ("RunLAr",datetime.datetime.now())
    if len(self.loaded_files) == 0:
      print('No files loaded with data dispatcher. Exiting gracefully')
      return

    if 'CLUSTER' in os.environ and 'PROCESS' in os.environ:
      cluster = os.environ['CLUSTER']
      process = os.environ['PROCESS']
    else:
      cluster = '0'
      process = '0'
    print ("RunLAr called with ",fcl,n,nskip)
    unused_files = []
      # new interface that does not talk to dd
    lar = LArWrapper.LArWrapper(debug=self.debug, fcl=fcl, o="temp.root", replicas=self.input_replicas, flist=self.lar_file_list, n=n, nskip=nskip, appFamily=self.appFamily, appName=self.appName, appVersion=self.appVersion, deliveryMethod="dd", workflowMethod=self.workflowMethod, projectID=self.proj_id, formatString="runLar_%s_%s_%%tc_%s_%s_%s.root")
    returncode = lar.DoLAr(cluster, process)
    self.unused_files = lar.LArResults()

    # make all files as bad if job crashed
    if returncode != 0:
      self.MarkFiles(True)
      print ("LAr returned", returncode)
      return returncode

    # else go through files and mark the ones closed in the logfile as good
    self.MarkFiles(False,self.unused_files)
    self.SaveFileDIDs()
    return returncode

""" driver test """

def driver(args):
    """ driver for DDInterface
    .. param: args (parsed arguments)
    .. type: []
    .. return: lar return code
    .. rtype: int
    """

    dd_interface = DDInterface( lar_limit=args.load_limit,
                             timeout=args.timeout,
                             wait_time=args.wait_time,
                             wait_limit=args.wait_limit,
                             appFamily=args.appFamily,
                             appName=args.appName,
                             appVersion=args.appVersion,
                             workflowMethod=args.workflowMethod,
                             namespace=args.namespace,
                             dataset=args.dataset)
    dd_interface.Login(args.user)
    dd_interface.SetWorkerID()
    print(os.environ['MYWORKERID'])
    dd_interface.AttachProject(dd_proj_id)
    #dd_interface.dump_project(dd_proj_id)
    dd_interface.LoadFiles()
    dd_interface.BuildFileListString()
    retcode = dd_interface.RunLAr(fcl=args.c, n=args.n, nskip=args.nskip)
    dd_interface.dump_project(dd_proj_id)
    return retcode

if __name__ == '__main__':
    ''' run_lar
    .. cmdoption:: --dataset <dataset>
    '''
    parser = ap()
    # dd args
    parser.add_argument('--dataset', default='schellma:run5141recentReco',type=str)
    parser.add_argument('--load_limit', default=1, type=int,help='number of files to give to lar')
    parser.add_argument('--namespace', default=None, type=str, help="optional namespace qualifier for dataset")
    parser.add_argument('--query_limit', default=100)
    parser.add_argument('--query_skip', default=10)
    parser.add_argument('--projectID', default=None, type=int, help="dd projectID, overrides dataset")
    parser.add_argument('--timeout', type=int, default=120)
    parser.add_argument('--wait_time', type=int, default=120)
    parser.add_argument('--wait_limit', type=int, default=5)
    parser.add_argument('--workflowMethod', type=str, default="batch", help= 'workflow method [interactive,batch,wfs]')
    # args shared with lar

    parser.add_argument('--appFamily',default='test', type=str, help=' application family')
    parser.add_argument('--appName', default='test',type=str, help=' application name')
    parser.add_argument('--appVersion', default=os.getenv('DUNESW_VERSION'), type=str, help='application version')
    parser.add_argument('--dataTier', default='out1:sam-user',type=str, help='samweb data tier for output file')
    parser.add_argument('--dataStream', default='out1:test',type=str, help='samweb data stream for output file')
    parser.add_argument('-o', default="temp.root", type=str, help='output event stream file')
    parser.add_argument('-c', required=True, type=str, help='name of fcl file')
    parser.add_argument('--user', default = os.getenv("USER"),type=str, help='user name')
    parser.add_argument('-n', type=int, default=10, help='number of events total to process')
    parser.add_argument('--nskip', type=int, default=0, help='number of events to skip before starting')
    parser.add_argument('--debug',type=bool,default=False,help= 'debug')
    args = parser.parse_args()




    print("-----------------------------------------------------------------------------------")
    print ("DDInterface arguments:\n")
    theargs = vars(args)
    for a in theargs:
        print(a,theargs[a])
    print ("-----------------------------------------------------------------------------------")

    if (not args.projectID) and args.dataset:
        dd_proj_id = submit_dd_jobs.create_project(dataset=args.dataset, namespace=args.namespace,
                                  query_limit=args.query_limit,
                                  query_skip=args.query_skip,debug=args.debug)

    elif args.projectID:
        dd_proj_id = int(args.projectID)
    else:
        print("Need to provide project OR dataset\n")
        sys.exit(1)

    retcode = driver(args)
    print ("run_lar returned:",retcode)
