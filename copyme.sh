export DWORK=dunegpvm03.fnal.gov:/dune/data/users/schellma/DataChallengeWork-loginator/
scp *.sh $DWORK
scp tests/*.sh $DWORK/tests/.
scp batch/* $DWORK/batch/.
scp python/*.py $DWORK/python/.
scp fcl/*.fcl $DWORK/fcl/.

