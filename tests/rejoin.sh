# run an interactive test HMS 12-2-2022
export BEGIN_TIME=`date  +"%d-%b-%Y %H:%M:%S %Z"`
python -m  DDInterface --projectID=347  --load_limit 3 -c eventdump.fcl --user schellma --appFamily=protoduneana  --appName=eventdump --appVersion=$PROTODUNEANA_VERSION  -n -1
