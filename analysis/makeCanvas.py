import ROOT
def makeCanvas(name):

    newcanvas = ROOT.TCanvas(name,name,1,1,600,600);
    ROOT.gStyle.SetOptStat(0);
    ROOT.gStyle.SetOptTitle(0);
    ROOT.gStyle.SetEndErrorSize(5)
    newcanvas.SetHighLightColor(2);
    #newcanvas.Range(-0.4285702,-2.714602e-39,2.428571,1.538274e-38);
    newcanvas.SetFillColor(0);
    newcanvas.SetBorderMode(0);
    newcanvas.SetBorderSize(2);
    newcanvas.SetLeftMargin(0.15);
    newcanvas.SetRightMargin(0.15);
    newcanvas.SetBottomMargin(0.15);
    newcanvas.SetFrameLineWidth(2);
    newcanvas.SetFrameBorderMode(0);
    newcanvas.SetFrameLineWidth(2);
    newcanvas.SetFrameBorderMode(0);
    return newcanvas

def makeMainPad(name="main"):
    newpad = ROOT.TPad(name,name,0.1,.1,.7,.7)
    return newpad

def makeLegendPad(name="legend"):
    newpad = ROOT.TPad(name,name,0.7,.1,.9,.7)
    return newpad
