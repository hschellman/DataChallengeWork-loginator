# run an interactive test HMS 12-2-2022
export BEGIN_TIME=`date  +"%d-%b-%Y %H:%M:%S %Z"`
python -m  DDInterface --projectID=385  --load_limit 2 -c eventdump.fcl --user $USER --workflowMethod="interactive" --appFamily=protoduneana  --appName=eventdump --appVersion=$PROTODUNEANA_VERSION  -n 10
