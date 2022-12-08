export PROJECTID=82
export PROCESSHASH="2e434568"
dd project create files from schellma:run5141recentReco limit 100
python LArWrapper.py --delivery_method=dd --processHASH=$PROCESSHASH\
                     --processID=0    -c dumpevent.fcl\
                     --user=$USER --projectID=$PROJECTID
