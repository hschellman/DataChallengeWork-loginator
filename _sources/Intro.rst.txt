What this package does
======================

  * Example scripts ( :doc:`tests` ) for running LArSoft using either `samweb <https://cdcvs.fnal.gov/redmine/projects/sam-web-client/wiki>`_ or the new `Data Dispatcher (dd) <https://data-dispatcher.readthedocs.io/en/latest/>`_ data delivery systems.


  * A log parser :doc:`generated/LArWrapper` that gathers information about the processing of an input file for monitoring purposes.

Instead of running `lar <https://larsoft.org/important-concepts-in-larsoft/>`_ directly,
you run :doc:`generated/LArWrapper` with similar arguments.  It produces a summary dictionary that include information about CPU and memory use.
This is useful both for profiling jobs before large processing campaigns and for monitoring overall resource use.

The package has subdirectoris:

======   ===================================
python   the code itself
batch    scripts needed for batch submission
tests    sample scripts to exercise the code
fcl      example fcl files
docs     documentation
======   ===================================


and some utility scripts to setup, tar up your code and make maintenance easier
