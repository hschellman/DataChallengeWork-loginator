#setup dunesw v09_54_00d00 -q e20:prof
export DATA_DISPATCHER_URL=https://metacat.fnal.gov:9443/dune/dd/data
export DATA_DISPATCHER_AUTH_URL=https://metacat.fnal.gov:8143/auth/dune
export METACAT_SERVER_URL=https://metacat.fnal.gov:9443/dune_meta_demo/app
export METACAT_AUTH_SERVER_URL=https://metacat.fnal.gov:8143/auth/dune
export PATH=/nashome/c/calcuttj/.local/bin/:$PATH
#export PYTHONPATH=/dune/app/users/calcuttj/data-dispatcher/data_dispatcher:$PYTHONPATH
#export PYTHONPATH=/dune/app/users/calcuttj/metacat3/metacat:$PYTHONPATH
export PYTHONPATH=/dune/app/users/calcuttj/dd_metacat_pip/venv/lib/python3.9/site-packages/:$PYTHONPATH
#export PYTHONPATH=/dune/app/users/calcuttj/dd_metacat_pip/venv/lib/python3.9/site-packages/:$PYTHONPATH

#source /dune/app/users/calcuttj/metacat3/metacat_venv/bin/activate
source /dune/app/users/calcuttj/dd_metacat_pip/venv/bin/activate

kx509
source ~/proxy.sh
export X509_USER_PROXY=/tmp/x509up_u1327
