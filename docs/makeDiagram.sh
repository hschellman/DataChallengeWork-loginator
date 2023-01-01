export TEMP=$PWD
cd $HERE/python
pydeps DDInterface.py -T jpg --include-missing --cluster --rankdir BT
mv DDInterface.jpg $HERE/docs
cd $TEMP
