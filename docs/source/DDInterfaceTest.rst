DDInterface tests
-----------------

Interactive
***********

Command line test of the DDInterface::

  python -m DDInterface --help
  usage: DDInterface.py [-h] [--dataset DATASET] [--load_limit LOAD_LIMIT] [--namespace NAMESPACE]
                        [--query_limit QUERY_LIMIT] [--query_skip QUERY_SKIP]
                        [--projectID PROJECTID] [--timeout TIMEOUT] [--wait_time WAIT_TIME]
                        [--wait_limit WAIT_LIMIT] [--workflowMethod WORKFLOWMETHOD]
                        [--appFamily APPFAMILY] [--appName APPNAME] [--appVersion APPVERSION]
                        [--dataTier DATATIER] [--dataStream DATASTREAM] [-o O] -c C [--user USER]
                        [-n N] [--nskip NSKIP] [--debug DEBUG]

  optional arguments:
    -h, --help            show this help message and exit
    --dataset DATASET
    --load_limit LOAD_LIMIT
                          number of files to give to lar
    --namespace NAMESPACE
                          optional namespace qualifier for dataset
    --query_limit QUERY_LIMIT
    --query_skip QUERY_SKIP
    --projectID PROJECTID
                          dd projectID, overrides dataset
    --timeout TIMEOUT
    --wait_time WAIT_TIME
    --wait_limit WAIT_LIMIT
    --workflowMethod WORKFLOWMETHOD
                          workflow method [interactive,batch,wfs]
    --appFamily APPFAMILY
                          application family
    --appName APPNAME     application name
    --appVersion APPVERSION
                          application version
    --dataTier DATATIER   samweb data tier for output file
    --dataStream DATASTREAM
                          samweb data stream for output file
    -o O                  output event stream file
    -c C                  name of fcl file
    --user USER           user name
    -n N                  number of events total to process
    --nskip NSKIP         number of events to skip before starting
    --debug DEBUG         debug
