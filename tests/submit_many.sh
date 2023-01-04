# submission tests
source tarme.sh  # make the tar file up-to-date
# run a test with more jobs than files to deliver
python -m submit_dd_jobs --debug=True --dataset=schellma:run5141Prod2Reco  --query_limit=20 --query_skip=10\
 -c fcl/test.fcl --njobs=10 -n 2 --load_limit=2 --appFamily=LArSoft --appName=test --appVersion=${DUNESW_VERSION}
