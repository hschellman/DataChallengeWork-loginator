import submit_dd_jobs
from run_lar import DDInterface
import sys
import os
from argparse import ArgumentParser as ap

if __name__ == '__main__':

    parser = ap()
    # dd args
    parser.add_argument('--dataset', default='schellma:run5141recentReco',type=str)
    parser.add_argument('--load_limit', default=4, type=int)
    parser.add_argument('--query_limit', default=10)
    parser.add_argument('--query_skip', default=0)
    parser.add_argument('--projectID', default=None)
    parser.add_argument('--timeout', type=int, default=120)
    parser.add_argument('--wait_time', type=int, default=120)
    parser.add_argument('--wait_limit', type=int, default=5)
    parser.add_argument('--workFlowMethod', type=int, default="batch", help= 'workflow method [interactive,batch,wfs]')
    # args shared with lar
   
    parser.add_argument('--appFamily',default='test', type=str, help=' application family')
    parser.add_argument('--appName', default='test',type=str, help=' application name')
    parser.add_argument('--appVersion', default=os.getenv('DUNESW_VERSION'), type=str, help='application version')
    parser.add_argument('--dataTier', default='out1:sam-user',type=str, help='data tier for output file')
    parser.add_argument('--dataStream', default='out1:test',type=str, help='data stream for output file')
    parser.add_argument('-o', default="temp.root", type=str, help='output event stream file')
    parser.add_argument('-c', required=True, type=str, help='name of fcl file')
    parser.add_argument('--user', default = os.getenv("USER"),type=str, help='user name')
    parser.add_argument('-n', type=int, default=10, help='number of events total to process')
    parser.add_argument('--nskip', type=int, default=0, help='number of events to skip before starting')
    args = parser.parse_args()

    if (not args.projectID) and args.dataset:
        dd_proj_id = submit_dd_jobs.create_project(dataset=args.dataset, namespace=None,
                                  query_limit=args.query_limit,
                                  query_skip=args.query_skip)


    elif args.project and not (args.dataset and args.namespace):
        dd_proj_id = int(args.project)
    else:
        sys.stderr.write("Need to provide project OR dataset & namespace\n")
        sys.exit(1)

    dd_interface = DDInterface( lar_limit=args.load_limit,
                             timeout=args.timeout,
                             wait_time=args.wait_time,
                             wait_limit=args.wait_limit,
                             appFamily=args.appFamily,
                             appName=args.appName,
                             appVersion=args.appVersion,
                             workflowMethod="interactive")
    dd_interface.Login(args.user)
    dd_interface.SetWorkerID()
    print(os.environ['MYWORKERID'])
    dd_interface.AttachProject(dd_proj_id)
    dd_interface.dump_project(dd_proj_id)
    dd_interface.LoadFiles()
    dd_interface.BuildFileListString()
    dd_interface.RunLAr(args.c, args.n, args.nskip)
    dd_interface.dump_project(dd_proj_id)
    ##Loginator stuff here?
