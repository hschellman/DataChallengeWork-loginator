#!/bin/bash
source /cvmfs/dune.opensciencegrid.org/products/dune/setup_dune.sh

export DATA_DISPATCHER_URL=https://metacat.fnal.gov:9443/dune/dd/data
export DATA_DISPATCHER_AUTH_URL=https://metacat.fnal.gov:8143/auth/dune
export METACAT_AUTH_SERVER_URL=https://metacat.fnal.gov:8143/auth/dune
export METACAT_SERVER_URL=https://metacat.fnal.gov:9443/dune_meta_demo/app


POSITIONAL_ARGS=()
nskip=0
output_rses=("DUNE_US_FNAL_DISK_STAGE")
get_min_rse=false
while [[ $# -gt 0 ]]; do
  case $1 in
    --namespace)
      NAMESPACE=$2
      shift
      shift 
      ;;
    --fcl)
      FCL=$2
      shift
      shift 
      ;;
    -n)
      N=$2
      shift
      shift
      ;;
    --load_limit)
      LOADLIMIT=$2
      shift
      shift 
      ;;
    --user)
      USER=$2
      shift
      shift 
      ;;
    --metacat_user)
      METACATUSER=$2
      shift
      shift 
      ;;
    --project)
      PROJECT=$2
      shift
      shift 
      ;;
    --timeout)
      DDTIMEOUT=$2
      shift
      shift 
      ;;
    --wait_time)
      WAITTIME=$2
      shift
      shift 
      ;;
    --wait_limit)
      WAITLIMIT=$2
      shift
      shift 
      ;;
    --output)
      OUTPUT=$2
      shift
      shift
      ;;
    --output_dataset)
      OUTPUTDATASET=$2
      shift
      shift
      ;;
    --output_namespace)
      OUTPUTNAMESPACE=$2
      shift
      shift
      ;;
    --nskip)
      nskip=$2
      shift
      shift
      ;;
    --appFamily)
      APPFAMILY=$2
      shift
      shift
      ;;
    --appName)
      APPNAME=$2
      shift
      shift
      ;;
    --appVersion)
      APPVERSION=$2
      shift
      shift
      ;;
    --rse)
      output_rses+=($2)
      shift
      shift
      ;;
    --min-rse)
      get_min_rse=true
      shift
      ;;
    *)
      POSITIONAL_ARGS+=("$1") # save positional arg
      shift # past argument
      ;;
  esac
done

echo $NAMESPACE

logname=loginator-${NAMESPACE}_${PROCESS}_${CLUSTER}_`date +%F_%H_%M_%S`

export PYTHONPATH=${CONDOR_DIR_INPUT}:${PYTHONPATH}

###Setting up dunesw/Data Dispatcher/MetaCat and running lar
(
setup dunesw v09_55_01d00 -q e20:prof;

python -m venv venv
source venv/bin/activate
pip install metacat
pip install datadispatcher

#source ${CONDOR_DIR_INPUT}/${MC_TAR}/canned_client_setup.sh
#source ${CONDOR_DIR_INPUT}/${DD_TAR}/canned_client_setup.sh

sleeptime=$[ ( $RANDOM % 120 ) ]
echo "Will sleep for ${sleeptime} seconds"
sleep $sleeptime

export MYWORKERID=`ddisp worker id -n`
echo "workerid: ${MYWORKERID}"

echo "I am in directory" ${PWD}

ls -lrt

echo "the input directory"

ls -lrt $CONDOR_DIR_INPUT

export FHICL_FILE_PATH=${CONDOR_DIR_INPUT}:${FHICL_FILE_PATH}

echo "try to really mean it about the FCL since putting it in the path doesn't seem to do it for me"

cp ${CONDOR_DIR_INPUT}/$FCL .

echo "I will now run run_lar with fcl file " $FCL

python -m run_lar \
  --namespace $NAMESPACE \
  --fcl $FCL \
  --project $PROJECT \
  --load_limit $LOADLIMIT \
  --user $USER \
  -n $N \
  --appFamily $APPFAMILY \
  --appName $APPNAME \
  --appVersion $APPVERSION \
  #--nskip $nskip \
  #> ${logname}.out 2>${logname}.err

returncode=$?
#echo "Return code: " $returncode >> ${logname}.out 2>>${logname}.err
echo "Return code: " $returncode
export SCRATCH_DIR=/pnfs/dune/scratch/users
#setup ifdh
export OUTDIR=${SCRATCH_DIR}/${USER}/ddtest/${PROJECT}

ifdh mkdir_p ${OUTDIR}
export IFDH_DEBUG=0
#if [ $? -ne 0 &&- z "$IFDH_OPTION"]; then
#    echo "Unable to read ${SCRATCH_DIR}/${USER}/ddtest make sure that you have created this directory and given it group write permission."
#    exit 74
#else
    # directory already exists, so let's copy
ls -lrt
env > env.txt
echo ${OUTDIR}
ls  > files.txt

echo "copy the following files to "${OUTDIR}
cat files.txt
ifdh cp -D $IFDH_OPTION *.json ${OUTDIR}
ifdh cp -D $IFDH_OPTION *.txt ${OUTDIR}
ifdh cp -D $IFDH_OPTION *.out ${OUTDIR}
ifdh cp -D $IFDH_OPTION *.err ${OUTDIR}
ifdh cp -D $IFDH_OPTION *.fcl ${OUTDIR}
#fi
)
echo "Site: $GLIDEIN_DUNESite" #>> ${logname}.out

if [ $returncode -ne 0 ]; then
  exit $returncode
fi



#if $get_min_rse; then
#  output_rses=(`python -m get_rse ${CONDOR_DIR_INPUT}/rses.txt`)
#fi
#echo "output rses"
#for rse in ${output_rses[@]}; do
#  echo $rse
#done
##if [ $returncode -ne "0" ]; then
##  echo "exiting";
##  exit;
##fi
#
####Setting up rucio, uploading to RSEs
#(
#setup rucio
#echo "PINGING"
#rucio ping
#echo "DONE PINGING"
#setup metacat
#
#export DATA_DISPATCHER_URL=https://metacat.fnal.gov:9443/dune/dd/data
#export DATA_DISPATCHER_AUTH_URL=https://metacat.fnal.gov:8143/auth/dune
#export METACAT_AUTH_SERVER_URL=https://metacat.fnal.gov:8143/auth/dune
#export METACAT_SERVER_URL=https://metacat.fnal.gov:9443/dune_meta_demo/app
#
#echo "Authenticating" #>> ${logname}.out 2>>${logname}.err
#metacat auth login -m x509 ${METACATUSER} # >> ${logname}.out 2>>${logname}.err
#date
#
#auth_return=$?
#if [ $auth_return -ne 0 ]; then
#  echo "could not declare to metacat"
#  exit $auth_return
#fi
#
#echo "whoami:" #>> ${logname}.out 2>>${logname}.err
#metacat auth whoami #>> ${logname}.out 2>>${logname}.err
#date
#
#
#parents=`cat loaded_files.txt`
#
#echo $OUTPUT #>> ${logname}.out 2>>${logname}.err
##output_files=`ls *$OUTPUT`
#shopt -s nullglob
#for i in *$OUTPUT; do
#  FILESIZE=`stat -c%s $i`
#  echo 'filesize ' ${FILESIZE} #>> ${logname}.out 2>>${logname}.err
#  cat << EOF > ${i}.json
#  [
#    {
#      "size": ${FILESIZE},
#      "namespace": "${OUTPUTNAMESPACE}",
#      "name": "${i}",
#      "metadata": {
#        "DUNE.campaign": "dc4",
#        "core.file_format": "root"
#      },
#      "parents": [
#        $parents
#      ]
#    }
#  ]
#EOF
#
#  metacat file declare -j ${i}.json $OUTPUTNAMESPACE:$OUTPUTDATASET-data #>> ${logname}.out 2>>${logname}.err
#  date
#  returncode=$?
#  if [ $returncode -ne 0 ]; then
#    echo "could not declare to metacat"
#    exit $returncode
#  fi
#
#  for rse in ${output_rses[@]}; do
#    echo "Uploading to $rse"
#    rucio -a dunepro upload --summary --scope $OUTPUTNAMESPACE --rse $rse $i #>> ${logname}.out 2>>${logname}.err
#    echo $?
#  done
#
#  #rucio -a dunepro upload --scope $OUTPUTNAMESPACE --rse DUNE_US_FNAL_DISK_STAGE $i #>> ${logname}.out 2>>${logname}.err
#  #rucio -a dunepro upload --scope $OUTPUTNAMESPACE --rse DUNE_CERN_EOS $i #>> ${logname}.out 2>>${logname}.err
#  rucio -a dunepro attach $OUTPUTNAMESPACE:$OUTPUTDATASET-data $OUTPUTNAMESPACE:$i #>> ${logname}.out 2>>${logname}.err
#done

#  FILESIZE=`stat -c%s ${logname}.out`
#  cat << EOF > ${logname}.out.json 
#  [
#    {
#      "size": ${FILESIZE},
#      "namespace": "${OUTPUTNAMESPACE}",
#      "name": "${logname}.out",
#      "metadata": {}
#    }
#  ]
#EOF
#  metacat file declare -j ${logname}.out.json $OUTPUTNAMESPACE:$OUTPUTDATASET-log
#  for rse in ${output_rses[@]}; do
#    echo "Uploading to $rse"
#    rucio -a dunepro upload --scope $OUTPUTNAMESPACE --rse $rse ${logname}.out
#  done
#
#  #rucio -a dunepro upload --scope $OUTPUTNAMESPACE --rse DUNE_US_FNAL_DISK_STAGE ${logname}.out
#  #rucio -a dunepro upload --scope $OUTPUTNAMESPACE --rse DUNE_CERN_EOS ${logname}.out
#  rucio -a dunepro attach $OUTPUTNAMESPACE:$OUTPUTDATASET-log $OUTPUTNAMESPACE:${logname}.out
#
#
#  FILESIZE=`stat -c%s ${logname}.err`
#  cat << EOF > ${logname}.err.json 
#  [
#    {
#      "size": ${FILESIZE},
#      "namespace": "${OUTPUTNAMESPACE}",
#      "name": "${logname}.err",
#      "metadata": {}
#    }
#  ]
#EOF
#  metacat file declare -j ${logname}.err.json $OUTPUTNAMESPACE:$OUTPUTDATASET-log
#  for rse in ${output_rses[@]}; do
#    echo "Uploading to $rse"
#    rucio -a dunepro upload --scope $OUTPUTNAMESPACE --rse $rse ${logname}.err
#  done
#  #rucio -a dunepro upload --scope $OUTPUTNAMESPACE --rse DUNE_US_FNAL_DISK_STAGE ${logname}.err
#  #rucio -a dunepro upload --scope $OUTPUTNAMESPACE --rse DUNE_CERN_EOS ${logname}.err
#  rucio -a dunepro attach $OUTPUTNAMESPACE:$OUTPUTDATASET-log $OUTPUTNAMESPACE:${logname}.err

#)
