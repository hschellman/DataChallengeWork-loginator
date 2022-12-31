# submission tests
source tarme.sh  # make the tar file up-to-date
# run a test with more jobs than files to deliver
python -m submit_dd_jobs --dataset=schellma:run5141recentReco  --query_limit=10 --query_skip=10\
 -c fcl/test.fcl --njobs=12 -n 10 --load_limit=1 --appFamily=LArSoft --appName=test --appVersion=${DUNESW_VERSION}
