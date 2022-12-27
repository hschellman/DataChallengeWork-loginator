import ROOT
from PlotUtils import MnvH1D,MnvLatErrorBand,MnvVertErrorBand,MnvH2D
from ROOT import TFile, TH1D,gROOT,gStyle,TColor,TCanvas,TPad,TMatrixD,TH2D

interesting = ["ppfx"]
#interesting = ["Muon Energy Rec."]
max =12

lownu = False
maxuniv = 1000
gROOT.Reset();
gStyle.SetOptStat("");
gStyle.SetOptFit(111)
gStyle.SetLineWidth(2)
#gROOT.SetStyle("Plain");
gStyle.SetLabelSize(0.04,"x");
gStyle.SetLabelSize(0.04,"y");
#gStyle.SetTitleFont(90);

#gStyle.SetPalette(69)
gStyle.SetPalette(69)

gStyle.SetPadColor(0);
gStyle.SetPadBorderMode(0);

gStyle.SetCanvasColor(0);
gStyle.SetCanvasBorderMode(0);

gStyle.SetFrameBorderMode(0);
gStyle.SetPadTickY(1)
gStyle.SetPadTickX(1)


gStyle.SetLegendFillColor(0);


t = TCanvas()
pad = t.GetPad(0)
pad.SetBottomMargin(0.15)
pad.SetLeftMargin(0.15)

tone = 255.
#red  = array('d',[ 180./tone, 190./tone, 209./tone, 223./tone, 204./tone, 228./tone, 205./tone, 152./tone,  91./tone])
#green = array('d',[  93./tone, 125./tone, 147./tone, 172./tone, 181./tone, 224./tone, 233./tone, 198./tone, 158./tone])
#blue  = array('d',[ 236./tone, 218./tone, 160./tone, 133./tone, 114./tone, 132./tone, 162./tone, 220./tone, 218./tone])
#stops = array('d',[ 0.00, 0.05,0.1,0.2,0.3,0.4,0.7,0.8,1.0])
#TColor.CreateGradientColorTable(9, stops, red, green, blue, 255, 0.5);


POTRAT = 8.923e19/1.59e20
header = "/minerva/data/users/schellmh/"
tail = "/cross_sections/eroica/cross_sections_muonpz_muonpt_lowangleqelike_minerva.root";
m111="bigrun_more_v35_mec1_phil1_rpa1_2017-08-29_2002_qelikelo"

filename = header+m111+tail
file = TFile(filename,"readonly")

file.Print()

interesting = ["ppfx"]

mc = {}
data = {}

#data[0] = MnvH2D()
data[0] = file.Get("cross_sections_muonpt_muonpz_data")
#data.Print("ALL")
bigcovmx = TMatrixD(96,96)
bigcovmx = data[0].GetTotalErrorMatrix(True,False)

mc[0] = MnvH2D(file.Get("cross_sections_muonpt_muonpz_mc"))

mcfile = file
datafile = file

vertnames = []
universes = {}
universenames = []
datahist={}
mchist = {}
covmx = {}
dataname = "cross_sections_muonpt_muonpz_data"
mcname = "cross_sections_muonpt_muonpz_mc"

for bin in range(0,1):
    entry = "%s"%(bin+1)
    
    mc[bin] = mcfile.Get(mcname)
    mc[bin].Scale(POTRAT)
    mc[bin].GetXaxis().SetRange(1,max)
    data[bin] = datafile.Get(dataname)
    data[bin].GetXaxis().SetRange(1,max)
    covmx[bin] = data[bin].GetTotalErrorMatrix(True,False)
    datahist[bin] = data[bin].GetCVHistoWithStatError().Clone()
    
    mc[bin].Print()
    mchist[bin] = mc[bin].GetCVHistoWithStatError().Clone()
    datahist[bin].Divide(mchist[bin])
    #data[bin].Print()
    universes[bin]={}
    names = mc[bin].GetVertErrorBandNames()
    
    for name in names:
        print name
        if name not in interesting:
            continue
        nhists = mc[bin].GetVertErrorBand(name).GetNHists()
        for hists in range(0,nhists):
            if hists > maxuniv: continue
            rename = "%s_%d"%(name,hists)
            #print "vert",rename
            universes[bin][rename]=mc[bin].GetVertErrorBand(name).GetHist(hists)
            universes[bin][rename].GetXaxis().SetRange(1,max)
            universes[bin][rename].Divide(mchist[bin])
            universenames.append(rename)
    names = mc[bin].GetLatErrorBandNames()
    for name in names:
        print name
        if name not in interesting:
            continue

        nhists = mc[bin].GetLatErrorBand(name).GetNHists()
        for hists in range(0,nhists):
            if hists > maxuniv: continue
            rename = "%s_%d"%(name,hists)
            #print "lat",rename
            universes[bin][rename]=mc[bin].GetLatErrorBand(name).GetHist(hists)
            universenames.append(rename)
            universes[bin][rename].GetXaxis().SetRange(1,max)
            universes[bin][rename].Divide(mchist[bin])
# have used it to normalize can now make it 1 as well
    mchist[bin].Divide(mchist[bin])
#vertnames = mc[0].GetVertErrorBandNames()

        
#print data
datahist[0].SetTitle(mc[0].GetTitle()+";Beam Energy;data/central value mc")
datahist[0].SetMinimum(0.5)
datahist[0].SetMaximum(2.0)
datahist[0].Draw("PE")

covmx[0].Print()

#mchist[0].SetTitle(mc[0].GetTitle()+";Beam Energy;data/mc")
mchist[0].Draw("SAME HIST")
mchist[0].Print("ALL")

universenames =  universes[0].keys()
print universenames
i = 1
for name in universes[0].keys():

    print "try ",name
    i = i+1
    if "ppfx" in name:
        universes[0][name].SetMarkerColor(2)
    if "Muon" in name:
        universes[0][name].SetMarkerColor(3)
    universes[0][name].SetMarkerStyle(22)
    universes[0][name].Draw("SAME PEX0 L HIST")
#    print universes[0][name].Print()

datahist[0].Draw("SAME")
mchist[0].Draw("SAME HIST")

save = "save"

if save == "save":
  outname = "flux_study_"+interesting[0]
  if lownu:
      outname = outname+"_lowNu"
  outname = outname+".pdf"
  outname = outname.replace(" ","_")
  pad.Print(outname)
  pad.Update()
  pad.Draw()
  ROOT.gROOT.SaveContext()







#--------------- end of the body of the program -------

## wait for input to keep the GUI (which lives on a ROOT event dispatcher) alive
if __name__ == '__main__':
  rep = ''
  while not rep in [ 'q', 'Q' ]:
      rep = raw_input( 'enter "q" to quit: ' )
      if 1 < len(rep):
          rep = rep[0]



