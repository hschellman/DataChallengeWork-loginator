Usage
=====

Installation
------------

To use Loginator, first install this package, and `metacat <https://metacat.readthedocs.io/en/latest/>`_ and `data dispatcher <https://data-dispatcher.readthedocs.io/en/latest/>`_ from github (or ups).


Then run `setup_hms` after modifying for your own user.

.. literalinclude:: ../../setup_hms.sh
   :linenos:
   :language: bash



Choose your delivery method
---------------------------

:doc:`generated/LArWrapper` and  :doc:`generated/DDInterface` set up data delivery projects using different interfaces and run over multiple files


Running over large datasets requires a data access system capable of delivering data from a set of files to multiple processes. This is implemented via

- a master *project* which creates and owns a list of files and keeps track

- a data catalog (samweb or metacat) that knows about data characteristics

- a data location catalog (samweb or rucio) that knows where a given file can be found

- a data delivery method, either a direct copy or xroot streaming from the location

- the process running on a worker node - it has an ID and is assigned files to process

The data dispatcher, wfs system and samweb all support the concept of projects and worker processe and act reasonably similarly.

This package currently supports batch and interactive runs of LArsoft using the new data dispatcher and interactive tests with the older samweb interface.

.. note:: wfs will be added soon.



Look at the tests
-----------------

-:doc:`tests`
