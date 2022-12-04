# run an interactive test HMS 12-2-2022
python run_interactive.py --dataset=schellma:run5141recentReco  --namespace=pdsp_det_reco --query_limit 100 --load_limit 3 --fcl eventdump.fcl --user schellma --appFamily=protoduneana --appVersion=$PROTODUNEANA_VERSION  -n 5
python run_interactive.py --project=82 --query_limit 100 --load_limit 5 --fcl eventdump.fcl --user schellma --appFamily=protoduneana --appVersion=$PROTODUNEANA_VERSION  -n 120


#metacat query -i "files from schellma:protodune-sp-physics-generic where (namespace=pdsp_det_reco and core.data_tier='full-reconstructed' and core.runs[any] in (5141))" > newid.txt
