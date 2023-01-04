from metacat.webapi import MetaCatClient
import sys
from time import sleep
import os
from data_dispatcher.api import DataDispatcherClient
from argparse import ArgumentParser as ap
import subprocess

def create_project(dataset=None, query=None, namespace = None, query_limit=None, query_skip=None, debug=False):
  mc_client = MetaCatClient('https://metacat.fnal.gov:9443/dune_meta_demo/app')
  dd_client = DataDispatcherClient(
    server_url='https://metacat.fnal.gov:9443/dune/dd/data',
    auth_server_url='https://metacat.fnal.gov:8143/auth/dune')
  dd_client.login_x509(os.environ['USER'],
                       os.environ['X509_USER_PROXY'])


  if (query != None and dataset != None):
      print ("Choose between query and dataset please")
      sys.exit(1)
  thequery = ""
  if query == None:
      if namespace != None:
          thequery = 'files from %s where namespace="%s" ordered'%(dataset, namespace)
      else:
          thequery = 'files from %s ordered'%(dataset)
  else:
      thequery +=  query + " ordered "

  print (thequery)

  if query_skip: thequery += ' skip %s'%query_skip
  if query_limit: thequery += ' limit %s'%query_limit

  print("------------------------createproject------------------------------")
  print("Start Project for :",thequery)
  #query metacat
  query_files = list(mc_client.query(thequery))
  count = 0
  for i in query_files:
      print (i)
      count +=1
      if count > 100:
          break

  if debug: print("create_project with", len(query_files), " files")

  #check size
  nfiles_in_dataset = len(query_files)
  if nfiles_in_dataset == 0:
    sys.stderr.write("Ignoring launch request on empty metacat query")
    sys.stderr.write("Query: %s"%thequery)
    sys.exit(1)

  #make project in data dispatcher
  proj_dict = dd_client.create_project(files=query_files, query=thequery, idle_timeout=259201)
  if debug: print("project dictionary",proj_dict)
  dd_proj_id = proj_dict['project_id']
  print('Project ID:', dd_proj_id)

  return dd_proj_id



def main():
  parser = ap()
  parser.add_argument('--project_only', action='store_true',help='just start the project')
  parser.add_argument('--dataset', type=str, default=None,help='dataset to run over')
  parser.add_argument('--namespace', type=str, default=None,help='file namespace within dataset (deprecated)')
  parser.add_argument('--query_limit', type=str, default=None,help='number of files to get in the query')
  parser.add_argument('--query_skip', type=str, default=None,help='number of files to skip in the query')
  parser.add_argument('--njobs', type=int, default=1,help='number of jobs to submit')

  parser.add_argument('--load_limit', type=int, default=None,help='number of files to give each process')
  parser.add_argument('-c', type=str, default='eventdump.fcl',help='the fcl file for art/lar')
  parser.add_argument('--fcl', type=str, default='eventdump.fcl',help='the fcl file for art/lar')
  parser.add_argument('-n', type=int, default=-1,help="number of events for lar")
  parser.add_argument('--output', type=str, default='"*reco*.root"',help='lar output argument "*reco*.root"')
  parser.add_argument('--output_dataset', type=str, default='test')
  parser.add_argument('--output_namespace', type=str, default='test')
  parser.add_argument('--metacat_user', type=str, default=os.getenv("USER"))
  parser.add_argument('--blacklist', type=str, nargs='+',help='bad sites to avoid')
  parser.add_argument('--projectID', type=int, default=None,help='join an existing project - alternative to dataset')
  parser.add_argument('--dry_run', action='store_true',help='just to setup')
  parser.add_argument('--appFamily', type=str)
  parser.add_argument('--appVersion', type=str)
  parser.add_argument('--appName', type=str)
  parser.add_argument('--debug',type=bool,default=False,help='do more printout')

  args = parser.parse_args()

  print ("submit_dd_jobs.py arguments")
  theargs = vars(args)
  for a in theargs:
    print (a,theargs[a])

  if args.c == None:
    args.c = args.fcl

  if args.appName == None:
      appName = args.c.replace(".fcl","")
  else:
      appName = args.appName

  if args.appVersion == None:
      appVersion = os.getenv("DUNESW_VERSION")
  else:
      appVersion = args.appVersion

  if args.appFamily == None:
      appFamily = "LArSoft"
  else:
      appFamily = args.appFamily

  mc_client = MetaCatClient('https://metacat.fnal.gov:9443/dune_meta_demo/app')
  dd_client = DataDispatcherClient(
    server_url='https://metacat.fnal.gov:9443/dune/dd/data',
    auth_server_url='https://metacat.fnal.gov:8143/auth/dune')
  dd_client.login_x509(os.environ['USER'],
                       os.environ['X509_USER_PROXY'])

  print(args.blacklist)

  if (not args.projectID) and args.dataset:
    ##build up query
    if args.namespace == None:
        query = 'files from %s ordered'%(args.dataset)
    else:
        query = 'files from %s where namespace="%s" ordered'%(args.dataset, args.namespace)
    if args.query_skip: query += ' skip %s'%args.query_skip
    if args.query_limit: query += ' limit %s'%args.query_limit
    print(query)
    #query metacat
    query_files = [i for i in mc_client.query(query)]
    #print(query_files)

    #check size
    nfiles_in_dataset = len(query_files)

    print ("got a list of ",nfiles_in_dataset," to run over")
    if nfiles_in_dataset == 0:
      sys.stderr.write("Ignoring launch request on empty metacat query")
      sys.stderr.write("Query: %s"%query)
      sys.exit(1)

    #make project in data dispatcher
    print(" try to create a project")
    try:
        proj_dict = dd_client.create_project(query_files, query=query,)
    except:
        print ("project creation failed, give up")
        sys.exit(1)
    dd_proj_id = proj_dict['project_id']
    print('Project ID:', dd_proj_id)

    if args.project_only:
      print('Only making project. Exiting now')
      exit()

  elif args.projectID and not (args.dataset):
    dd_proj_id = args.projectID
  else:
    sys.stderr.write("submit_dd_jobs: Need to provide project OR dataset & optional namespace\n")
    sys.exit(1)

  if args.njobs > 10000:
    njobs = [10000]*int(args.njobs/10000) + [args.njobs%10000]
  else:
    njobs = [args.njobs]

  print(njobs)
  count = 0
  for nj in njobs:
    cmd =  'fife_launch -c $TESTME/batch/ddconfig.cfg ' \
          f'-Oglobal.load_limit={args.load_limit} ' \
          f'-Oglobal.projectID={dd_proj_id} ' \
          f'-Oglobal.n={args.n} ' \
          f'-Oglobal.output={args.output} ' \
          f'-Oglobal.output_dataset={args.output_dataset} ' \
          f'-Oglobal.output_namespace={args.output_namespace} ' \
          f'-Osubmit.N={nj} ' \
          f'-Oglobal.metacat_user={args.metacat_user} '\
          f'-Oglobal.appFamily={args.appFamily} '\
          f'-Oglobal.appName={args.appName} '\
          f'-Oglobal.appVersion={args.appVersion} '\
          f'-Oglobal.fcl={args.c} '\
          f'-Oglobal.debug={args.debug}'

    if args.blacklist:
      cs_blacklist = ','.join(args.blacklist)
      cmd += f'-Osubmit.blacklist={cs_blacklist} '

    if args.dry_run:
      cmd += '--dry_run '
    print("submit command:",cmd)

    subprocess.run(cmd, shell=True)

    if count < len(njobs)-1:
      print('Sleeping')
      for i in range(60):
        sleep(2)
        print(f'{i*2}/120', end='\r')
    count += 1

if __name__ == '__main__':
    main()
