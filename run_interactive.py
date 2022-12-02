import submit_dd_jobs
from run_lar import DDInterface 
import sys
import os
from argparse import ArgumentParser as ap

if __name__ == '__main__':

  parser = ap()
  parser.add_argument('--namespace', type=str)
  parser.add_argument('--dataset', type=str)
  parser.add_argument('--fcl', type=str)
  parser.add_argument('--load_limit', type=int)
  parser.add_argument('--query_limit', default=10)
  parser.add_argument('--query_skip', default=None)
  parser.add_argument('--user', type=str)
  parser.add_argument('--project', default=None)
  parser.add_argument('--timeout', type=int, default=120)
  parser.add_argument('--wait_time', type=int, default=120)
  parser.add_argument('--wait_limit', type=int, default=5)
  parser.add_argument('-n', type=int, default=-1)
  parser.add_argument('--nskip', type=int, default=0)
  args = parser.parse_args()

  if (not args.project) and args.dataset and args.namespace:
    dd_proj_id = submit_dd_jobs.create_project(namespace=args.namespace,
                                  dataset=args.dataset,
                                  query_limit=args.query_limit,
                                  query_skip=args.query_skip)


  elif args.project and not (args.dataset and args.namespace):
    dd_proj_id = int(args.project)
  else:
    sys.stderr.write("Need to provide project OR dataset & namespace\n")
    sys.exit(1)

  dd_interface = DDInterface(args.namespace,
                             args.load_limit,
                             timeout=args.timeout,
                             wait_time=args.wait_time,
                             wait_limit=args.wait_limit)
  dd_interface.Login(args.user)
  dd_interface.SetWorkerID()
  print(os.environ['MYWORKERID'])
  dd_interface.AttachProject(dd_proj_id)
  dd_interface.LoadFiles()
  dd_interface.BuildFileListString()
  dd_interface.RunLAr(args.fcl, args.n, args.nskip)

  ##Loginator stuff here?
 
