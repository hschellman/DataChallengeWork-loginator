# run an interactive test HMS 12-2-2022
export BEGIN_TIME=`date +"%d-%b-%Y %H:%M:%S %Z"`
python -m DDInterface --debug=True --dataset schellma:run5141recentReco --query_limit 10 --load_limit 1 -c fcl/test.fcl --user $USER --appFamily=protoduneana --appName=test --appVersion=$PROTODUNEANA_VERSION  -n 2
# this does a minimal test reading n=20 events from load_limit=1 files
