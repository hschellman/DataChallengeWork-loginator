metacat query "files from dune:all where namespace=np04_pdspprod2_reco and core.file_type=detector and core.run_type='protodune-sp' and \
core.data_tier=full-reconstructed and core.data_stream=physics and core.runs[any] in (5141)"

metacat dataset create -f "files from dune:all where namespace=np04_pdspprod2_reco and core.file_type=detector and core.run_type='protodune-sp' and \
core.data_tier=full-reconstructed and core.data_stream=physics and core.runs[any] in (5141)" schellma:run5141Prod2Reco "Prod2 Run 5141"

samweb list-files --summary "run_type=protodune-sp and file_type=mc and data_tier=full-reconstructed and DUNE_MC.beam_energy=1.0 and DUNE.Campaign=PDSPProd4a"

metacat query "files from dune:all where namespace='pdsp_mc_reco' and core.file_type=mc and core.data_tier='full_reconstructed' and 

metacat query  "files from dune:all where namespace='pdsp_mc_reco' and core.file_type=mc and core.data_tier='full-reconstructed' and DUNE.campaign=PDSPProd4a and MC.space_charge=yes"

metacat dataset create -f "files from dune:all where namespace='pdsp_mc_reco' and core.file_type=mc and core.data_tier='full-reconstructed' and DUNE.campaign=PDSPProd4a and MC.space_charge=yes" schellma:PDSPProd4a_mc_1GeV_sce  "files from dune:all where namespace='pdsp_mc_reco' and core.file_type=mc and core.data_tier='full-reconstructed' and DUNE.campaign=PDSPProd4a and MC.space_charge=yes"
