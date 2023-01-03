export DWORK=dunegpvm03.fnal.gov:/dune/data/users/schellma/LArWrapper
scp *.sh $DWORK
scp tests/*.sh $DWORK/tests/.
scp batch/* $DWORK/batch/.
scp python/*.py $DWORK/python/.
scp fcl/*.fcl $DWORK/fcl/.

