.. reliquery documentation master file, created by
   sphinx-quickstart on Tue Oct 19 10:55:01 2021.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to reliquery's documentation!
=====================================

.. toctree::
   :maxdepth: 2
   :caption: Contents:

.. rubric:: Modules

.. autosummary::
   :toctree: generated

   reliquery.relic.Relic
   reliquery.relic.Reliquery


.. Indices and tables
.. ==================

.. * :ref:`genindex`
.. * :ref:`modindex`
.. * :ref:`search`


##################
What is Reliquery?
##################

Reliquery is a tool developed in the pursuit of scientific exploration. It simplifies
the storage of research artifacts making them easy to access and share.

1. :ref:`Getting Started`
2. :ref:`Tags`
3. :ref:`Reliquery`

Getting Started 
###############

To install Reliquery:

.. code-block::

   $ pip install reliquery

Once installed you can use the Reliquery api in your jupyter notebooks or python scripts. To do
this create a Relic and start adding data.

.. code-block:: python

   from reliquery import Relic
   import numpy as np

   # create a Relic object
   relic = Relic(name="intro", relic_type="tutorial")

   #add a numpy array
   relic.add_array(name="ones", np.ones((10, 10))

   # add text 
   relic.add_text(
      name="relic description",
      text="This relic is a canvas to add and get data"
   )

Adding data to a relic follows simple format: add_<data type>(name, data). Getting and listing 
the data stored on a Relic follow similar method naming conventions.

.. code-block:: python

   relic.list_arrays()
   # output: ['ones']

   relic.get_array(name="ones").shape
   # output: (10, 10)

Out of the box Reliquery is configured by default to store data on a local file system. You can
find a */reliquery* directory in your home directory. This is were relics and their artifacts are
stored. Reliquery does support the use of **AWS s3** and **Dropbox** storage solutions alongside 
local file storage. This is done by adding a config file to the reliquery directory that details
the storages used.

.. code-block:: python

   # ~/reliquery/config
   {
      "default": {
         "storage": {
            "type": "File",
            "args": {
                "root": "~/reliquery"
            }
         }
      },
    "s3": {
         "storage": {
            "type": "S3",
            "args": {
               "s3_signed": true,
               "s3_bucket": "reliquery",
               "prefix": "relics"
            }
         }
      },
      "dropbox":{
         "storage": {
            "type": "Dropbox",
            "args": {
               "access_token": <acces_token>,
               "prefix": "relics"
            }
         }
      }
   }

The keys in the config file are used to tell Reliquery which storage to use when
adding and getting data from a Relic.

.. code-block:: python

   local_relic = Relic(name="local", relic_type="tutorial")
   # storage_name defaults to 'default'

   s3_relic = Relic(
      name="s3", relic_type="tutorial", storage_name="s3"
   )

   dropbox_relic = Relic(
      name="dropbox", relic_type="tutorial", storage_name="dropbox"
   )

**Notes:** 

* For s3 you will need to have AWS credentials stored on your local machine.
  Can be stored at *~/.aws/credentials* or where ever you decided. Checkout 
  `boto3 docs <https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html>`_ for more info.

* Storage names can be anything you want but they need to match the keys in your config file. 

Tags
####

Relics can be tagged allowing users to add descriptors that give extra information about a Relic.
These can be used to describe relationships between Relics and used to query relics as well. 

.. code-block:: python

   relic = Relic(name="intro", relic_type="tutorial")

   relic.add_tag(
      {
         "author": "tesla",
         "pre-process": "nmf",
         "components": "2"
      }
   )


Reliquery
#########

To query over Relics we have the Reliquery class that handles syncing all available relics. 
Using this class we can get Relics by their tags defined. 

.. code-block:: python

   # Creating a Reliquery object scans 
   # for available relics accross storages.
   rq = Reliquery()

   # A list of Relic objects returned from query
   relics = rq.get_relics_by_tag(key="pre-process", value="nmf")
   


