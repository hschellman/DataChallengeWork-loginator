# submission tests
source tarme.sh  # make the tar file up-to-date
python -m submit_dd_jobs --dataset=schellma:run5141recentReco  --namespace=pdsp_det_reco --query_limit=50\
 -c fcl/test.fcl --njobs=35 -n -1 --load_limit=2 --appFamily=LArSoft --appName=test --appVersion=${DUNESW_VERSION}
