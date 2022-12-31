# submission tests
source tarme.sh  # make the tar file up-to-date
#python -m submit_dd_jobs --debug=True --dataset=schellma:run5141recentReco --query_limit=10 -c fcl/test.fcl -n 10 --njobs=1  --load_limit=2 --appFamily=LArSoft --appName=test --appVersion=${DUNESW_VERSION}
python -m submit_dd_jobs --debug=True --dataset=schellma:run5141Prod2Reco --query_limit=10 -c fcl/test.fcl -n 10 --njobs=1  --load_limit=2 --appFamily=LArSoft --appName=test --appVersion=${DUNESW_VERSION}
