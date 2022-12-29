# setup for running data dispatcher tests
export DATA_DISPATCHER_URL=https://metacat.fnal.gov:9443/dune/dd/data
export DATA_DISPATCHER_AUTH_URL=https://metacat.fnal.gov:8143/auth/dune
export METACAT_SERVER_URL=https://metacat.fnal.gov:9443/dune_meta_demo/app
export METACAT_AUTH_SERVER_URL=https://metacat.fnal.gov:8143/auth/dune
export PATH=$HOME/.local/bin/:$PATH
export HERE=$PWD
export PYTHONPATH=$HERE/python:${PYTHONPATH}
kx509
source ~/proxy.sh
export X509_USER_PROXY=/tmp/x509up_u1327  # this is specific to me
