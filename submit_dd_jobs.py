from metacat.webapi import MetaCatClient
import sys
from time import sleep
import os
from data_dispatcher.api import DataDispatcherClient
from argparse import ArgumentParser as ap
import subprocess

def create_project(dataset, namespace, query_limit=None, query_skip=None):
  mc_client = MetaCatClient('https://metacat.fnal.gov:9443/dune_meta_demo/app')
  dd_client = DataDispatcherClient(
    server_url='https://metacat.fnal.gov:9443/dune/dd/data',
    auth_server_url='https://metacat.fnal.gov:8143/auth/dune')
  dd_client.login_x509(os.environ['USER'],
                       os.environ['X509_USER_PROXY'])

  #query = 'files from %s where namespace="%s" ordered'%(dataset, namespace)
  query = 'files from %s ordered'%(dataset)
  if query_skip: query += ' skip %s'%query_skip
  if query_limit: query += ' limit %s'%query_limit
  print("Start Project for :",query)
  #query metacat
  query_files = [i for i in mc_client.query(query)]
  #print(query_files)

  #check size
  nfiles_in_dataset = len(query_files)
  if nfiles_in_dataset == 0:
    sys.stderr.write("Ignoring launch request on empty metacat query")
    sys.stderr.write("Query: %s"%query)
    sys.exit(1)

  #make project in data dispatcher
  proj_dict = dd_client.create_project(query_files, query=query)
  dd_proj_id = proj_dict['project_id']
  print('Project ID:', dd_proj_id)

  return dd_proj_id



if __name__ == '__main__':
  parser = ap()
  parser.add_argument('--project_only', action='store_true')
  parser.add_argument('--dataset', type=str, default=None)
  parser.add_argument('--namespace', type=str, default=None)
  parser.add_argument('--query_limit', type=str, default=None)
  parser.add_argument('--query_skip', type=str, default=None)
  parser.add_argument('--njobs', type=int, default=1)

  parser.add_argument('--load_limit', type=int, default=None)
  parser.add_argument('--fcl', type=str, default='eventdump.fcl')
  parser.add_argument('--nevents', type=int, default=-1)
  parser.add_argument('--output_str', type=str, default='"*reco.root"')
  parser.add_argument('--output_dataset', type=str, default='dd-interactive-tests')
  parser.add_argument('--output_namespace', type=str, default='dc4-hd-protodune')
  parser.add_argument('--metacat_user', type=str, default='schellma')
  parser.add_argument('--blacklist', type=str, nargs='+')
  parser.add_argument('--project', type=int, default=None)
  parser.add_argument('--dry_run', action='store_true')
  parser.add_argument('--appFamily', type=str)
  parser.add_argument('--appVersion', type=str)
  parser.add_argument('--appName', type=str)
  
  args = parser.parse_args()

  if args.appName == None:
      appName = args.fcl.replace(".fcl","")
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

  if (not args.project) and args.dataset and args.namespace:
    ##build up query
    query = 'files from %s where namespace="%s" ordered'%(args.dataset, args.namespace)
    if args.query_skip: query += ' skip %s'%args.query_skip
    if args.query_limit: query += ' limit %s'%args.query_limit
    print(query)
    #query metacat
    query_files = [i for i in mc_client.query(query)]
    #print(query_files)

    #check size
    nfiles_in_dataset = len(query_files)
    if nfiles_in_dataset == 0:
      sys.stderr.write("Ignoring launch request on empty metacat query")
      sys.stderr.write("Query: %s"%query)
      sys.exit(1)

    #make project in data dispatcher
    proj_dict = dd_client.create_project(query_files, query=query)
    dd_proj_id = proj_dict['project_id']
    print('Project ID:', dd_proj_id)

    if args.project_only:
      print('Only making project. Exiting now')
      exit()

  elif args.project and not (args.dataset and args.namespace):
    dd_proj_id = args.project
  else:
    sys.stderr.write("Need to provide project OR dataset & namespace\n")
    sys.exit(1)

  if args.njobs > 10000:
    njobs = [10000]*int(args.njobs/10000) + [args.njobs%10000]
  else:
    njobs = [args.njobs]

  print(njobs)
  count = 0
  for nj in njobs:
    cmd =  'fife_launch -c ddconfig.cfg ' \
          f'-Oglobal.load_limit={args.load_limit} ' \
          f'-Oglobal.project={dd_proj_id} ' \
          f'-Oglobal.nevents={args.nevents} ' \
          f'-Oglobal.output_str={args.output_str} ' \
          f'-Oglobal.output_dataset={args.output_dataset} ' \
          f'-Oglobal.output_namespace={args.output_namespace} ' \
          f'-Osubmit.N={nj} ' \
          f'-Oglobal.metacat_user={args.metacat_user} '\
          f'-Oglobal.appFamily={args.appFamily} '\
          f'-Oglobal.appName={args.appName} '\
          f'-Oglobal.appVersion={args.appVersion} '\
          f'-Oglobal.fcl={args.fcl} '

    if args.blacklist:
      cs_blacklist = ','.join(args.blacklist)
      cmd += f'-Osubmit.blacklist={cs_blacklist} '

    if args.dry_run:
      cmd += '--dry_run '
    print("submit command:",cmd)
    #cmd2 = ('fife_launch -c byhand.cfg '
    #        '-Oglobal.load_limit=%i '
    #        '-Oglobal.project=%s '
    #        '-Oglobal.nevents=%i '
    #        '-Oglobal.output_str=%s '
    #        '-Oglobal.output_dataset=%s '
    #        '-Oglobal.output_namespace=%s '
    #        '-Osubmit.N=%i '
    #        '-Oglobal.metacat_user=%s '
    #        )%(args.load_limit, dd_proj_id, args.nevents,
    #           args.output_str, args.output_dataset, args.output_namespace,
    #           nj, args.metacat_user)

    #print(cmd == cmd2)
    #print(cmd)
    #print(cmd2)

    subprocess.run(cmd, shell=True)
    #subprocess.run(('fife_launch -c byhand.cfg '
    #                '-Oglobal.load_limit=%i '
    #                '-Oglobal.project=%s '
    #                '-Oglobal.nevents=%i '
    #                '-Oglobal.output_str=%s '
    #                '-Oglobal.output_dataset=%s '
    #                '-Oglobal.output_namespace=%s '
    #                '-Osubmit.N=%i '
    #                '-Oglobal.metacat_user=%s '
    #               )%(args.load_limit, dd_proj_id, args.nevents,
    #                  args.output_str, args.output_dataset, args.output_namespace,
    #                  nj, args.metacat_user),
    #               shell=True)

    if count < len(njobs)-1:
      print('Sleeping')
      for i in range(60):
        sleep(2)
        print(f'{i*2}/120', end='\r')
    count += 1
