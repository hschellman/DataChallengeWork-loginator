# submission tests
source tarme.sh  # make the tar file up-to-date
python -m submit_dd_jobs --dataset=schellma:run5141recentReco  --namespace=pdsp_det_reco --query_limit=50\
 -c eventdump.fcl --njobs=35 -n -1 --load_limit=2 --appFamily=LArSoft --appName=test --appVersion=${DUNESW_VERSION}

#--dataset DATASET] [--namespace NAMESPACE] [--query_limit QUERY_LIMIT]
#                         [--query_skip QUERY_SKIP] [--njobs NJOBS] [--load_limit LOAD_LIMIT] [--fcl FCL] [--nevents NEVENTS]
#                         [--output_str OUTPUT_STR] [--output_dataset OUTPUT_DATASET] [--output_namespace OUTPUT_NAMESPACE]
#                         [--metacat_user METACAT_USER] [--blacklist BLACKLIST [BLACKLIST ...]] [--project PROJECT] [--dry_run]
