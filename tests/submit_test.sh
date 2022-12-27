# submission tests
source tarme.sh  # make the tar file up-to-date
python -m submit_dd_jobs --debug=True --dataset=schellma:run5141recentReco --query_limit=100 -c eventdump.fcl -n 1 --njobs=1  --load_limit=2 --appFamily=LArSoft --appName=eventdump --appVersion=${DUNESW_VERSION}
