from argparse import ArgumentParser as ap
import sys
import os
import subprocess
import time
import datetime
import requests

from data_dispatcher.api import DataDispatcherClient
from data_dispatcher.api import APIError

import Loginator

def call_and_retry(func):
  def inner1(*args, **kwargs):
    nretries = 0
    while nretries < 5:
      try:
        print(datetime.datetime.now())
        func(*args, **kwargs)
        break
      except (requests.exceptions.ConnectionError, APIError) as err:
        print('Caught', err.args)
        print(f'Will wait {args[0].retry_time} seconds and try again')
        time.sleep(args[0].retry_time)
        nretries += 1
    if nretries > 4:
      print('Too many retries')
      sys.exit(1)
  return inner1

def call_and_retry_return(func):
  def inner1(*args, **kwargs):
    nretries = 0
    while nretries < 5:
      try:
        print(datetime.datetime.now())
        result = func(*args, **kwargs)
        break
      except (requests.exceptions.ConnectionError, APIError) as err:
        print('Caught', err.args)
        print(f'Will wait {args[0].retry_time} and try again')
        time.sleep(args[0].retry_time)
        nretries += 1
    if nretries > 4:
      print('Too many retries')
      sys.exit(1)
    return result
  return inner1

class DDInterface:
  def __init__(self, namespace, lar_limit, timeout=120, wait_time=60, wait_limit=5):
    self.dataset = "" #dataset
    self.limit = 1#limit
    self.namespace = namespace
    query_args = (self.dataset, self.namespace, self.limit)
    self.query = '''files from %s where namespace="%s" limit %i'''%query_args
    self.worker_timeout = 3600*5
    self.lar_limit = lar_limit
    self.proj_id = -1
    self.proj_exists = False
    self.proj_state = None
    self.loaded_files = []
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

    self.retry_time = 600

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
    self.dd_client.login_x509(username, os.environ['X509_USER_PROXY'])
    #print(datetime.datetime.now())


  def SetLarLimit(self, limit):
    self.lar_limit = limit

  def CreateProject(self):
    query_files = mc_client.query(self.query)
    proj_dict = self.dd_client.create_project(
        query_files, query=self.query, worker_timeout=self.worker_timeout)
    print(datetime.datetime.now())
    self.proj_state = proj_dict['state']
    self.proj_id = proj_dict['project_id']
    self.proj_exists = True
    print(proj_dict)

  def PrintFiles(self):
    print('Printing files')
    for j in self.loaded_files:
      print(j['name'])

  @call_and_retry
  def next_file(self):
    self.next_output = self.dd_client.next_file(
        self.proj_id, timeout=self.dd_timeout,
        worker_id=os.environ['MYWORKERID'])
    #print(datetime.datetime.now())

  @call_and_retry
  def file_done(self, did):
    #self.dd_client.file_done(self.proj_id, '%s:%s'%(j['namespace'], j['name']))
    self.dd_client.file_done(self.proj_id, did)
    #print(datetime.datetime.now())

  @call_and_retry
  def file_failed(self, did, do_retry=True):
    self.dd_client.file_failed(
        self.proj_id, did,
        #'%s:%s'%(self.next_output['namespace'], self.next_output['name']),
        retry=do_retry)
    #print(datetime.datetime.now())

  @call_and_retry_return
  def get_project(self, proj_id):
    proj = self.dd_client.get_project(proj_id, with_files=False)
    #print(datetime.datetime.now())
    return proj

  def LoadFiles(self):
    count = 0
    ##Should we take out the proj_state clause?
    while (count < self.lar_limit and not self.next_failed): #and
           #self.proj_state == 'active'):
      print('Attempting fetch %i/%i'%(count, self.lar_limit), self.next_failed)
      self.Next()
      if self.next_output == None:
        ## this shouldn't happen, but if it does just exit the loop
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
        if len(self.next_replicas) > 0:
          self.loaded_files.append(self.next_output)
          count += 1
          ##also reset the number of times waited
          self.n_waited = 0
        else:
          print('Empty replicas -- marking as failed')
          self.file_failed(
              '%s:%s'%(self.next_output['namespace'], self.next_output['name']),
              do_retry=False)
    self.loaded = True
    print("Loaded %i files. Moving on."%len(self.loaded_files))
    self.PrintFiles()

  def Next(self):
    if self.proj_id < 0:
      raise ValueError('DDLArInterface::Next -- Project ID is %i. Has a project been created?'%self.proj_id)
    ## exists, state, etc. -- TODO
    self.next_file()
    if self.next_output == None:
      self.next_failed = True
      return

    if type(self.next_output) == dict:
      self.next_replicas = list(self.next_output['replicas'].values())


  def MarkFiles(self, failed=False):
    state = 'failed' if failed else 'done'
    #nretries = 0
    for j in self.loaded_files:
      if failed:
        print('Marking failed')
        self.file_failed('%s:%s'%(j['namespace'], j['name']))
      else:
        print('Marking done')
        self.file_done('%s:%s'%(j['namespace'], j['name']))

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
      if len(replicas) > 0:
        #Get the first replica
        replica = replicas[0]
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
  def RunLAr(self, fcl, n, nskip):
    if len(self.loaded_files) == 0:
      print('No files loaded with data dispatcher. Exiting gracefully')
      return

    if 'CLUSTER' in os.environ and 'PROCESS' in os.environ:
      cluster = os.environ['CLUSTER']
      process = os.environ['PROCESS']
    else:
      cluster = '0'
      process = '0'
    ## TODO -- make options for capturing output
    fname = "dc4_hd_protodune_%s_%s_reco.root"%(cluster, process)
    oname = fname.replace(".root",".out")
    ename = fname.replace(".root",".err")
    ofile = open(oname,'w')
    efile = open(ename,'w')
    proc = subprocess.run('lar -c %s -s %s -n %i --nskip %i -o fname'%(fcl, self.lar_file_list, n, nskip), shell=True, stdout=ofile,stderr=efile)
    ofile.close()
    efile.close()
    logparse = Loginator.Loginator(oname)
    logparse.readme()
    logparse.addinfo(logparse.getinfo())
    logparse.addmetacatinfo(self.namespace)  #HMS assuming this is the input namespace.
    logparse.writeme()
    if proc.returncode != 0:
      self.MarkFiles(True)
      sys.exit(proc.returncode)
    self.MarkFiles()
    self.SaveFileDIDs()


if __name__ == '__main__':

  parser = ap()
  parser.add_argument('--namespace', type=str)
  parser.add_argument('--fcl', type=str)
  parser.add_argument('--load_limit', type=int)
  parser.add_argument('--user', type=str)
  parser.add_argument('--project', type=int)
  parser.add_argument('--timeout', type=int, default=120)
  parser.add_argument('--wait_time', type=int, default=120)
  parser.add_argument('--wait_limit', type=int, default=5)
  parser.add_argument('-n', type=int, default=-1)
  parser.add_argument('--nskip', type=int, default=0)
  args = parser.parse_args()

  dd_interface = DDInterface(args.namespace,
                             args.load_limit,
                             timeout=args.timeout,
                             wait_time=args.wait_time,
                             wait_limit=args.wait_limit)
  dd_interface.Login(args.user)
  dd_interface.AttachProject(args.project)
  dd_interface.LoadFiles()
  dd_interface.BuildFileListString()
  dd_interface.RunLAr(args.fcl, args.n, args.nskip)


