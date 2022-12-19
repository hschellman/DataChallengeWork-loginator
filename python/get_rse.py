import subprocess
import sys
import os

if 'GLIDEIN_DUNESite' not in os.environ.keys():
  glidein_site = 'US_FNAL'
else:
  glidein_site = os.environ['GLIDEIN_DUNESite']
def is_good_rse(rse):
  return (('DUNE_CERN_EOS' not in rse) and
          ('FNAL_DCACHE' not in rse) and
          ('CERN_PDUNE_CASTOR' not in rse) and
          ('CERN_PDUNE_EOS' not in rse))
with open(sys.argv[1], 'r') as f:
  rses = {i.split(',')[1]:int(i.split(',')[2].strip('\n')) for i in f.readlines() if (glidein_site in i and is_good_rse(i.split(',')[1]))}
#print(rses)

print(min(rses, key=rses.get))
