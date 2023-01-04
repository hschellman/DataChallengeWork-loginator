# run an interactive test HMS 12-2-2022
export BEGIN_TIME=`date  +"%d-%b-%Y %H:%M:%S %Z"`
python -m  DDInterface --projectID=403  --load_limit 1 -c fcl/test.fcl --user $USER --workflowMethod="interactive" --appFamily=protoduneana  --appName=test --appVersion=$PROTODUNEANA_VERSION  -n 2
