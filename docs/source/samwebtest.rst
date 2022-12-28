Samweb test
-----------

To run a test using samweb
you can use the ``samtest`` module:

the command line API is::

  python -m samtest --help
  usage: samtest.py [-h] [--defName DEFNAME] [--appFamily APPFAMILY] [--appName APPNAME]
                    [--appVersion APPVERSION] [--dataTier DATATIER] [--dataStream DATASTREAM]
                    [-o O] [-c C] [--user USER] [-n N] [-nskip NSKIP] [--maxFiles MAXFILES] [-M M]

  optional arguments:
    -h, --help            show this help message and exit
    --defName DEFNAME     samweb dataset definition name
    --appFamily APPFAMILY
                          samweb needs this
    --appName APPNAME     samweb needs this
    --appVersion APPVERSION
                          samweb needs this
    --dataTier DATATIER   data tier for output file
    --dataStream DATASTREAM
                          data stream for output file
    -o O                  output event stream file
    -c C                  name of fcl file
    --user USER           user name
    -n N                  number of events total to process
    -nskip NSKIP          number of events to skip before starting
    --maxFiles MAXFILES   number of files to deliver per job
