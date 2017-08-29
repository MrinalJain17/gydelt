Welcome to gydelt
===============
It's pretty complicated to get data from GDELT. Not only accessing the data but preprocessing the data is also a tedious task. **'gydelt'**  is a wrapper that can be used to access and perform the basic preprocessing operations on the data obtained from GDELT.

[View Full Documentation of gydelt](http://gydelt.readthedocs.io/en/latest/)


----------


Why use gydelt?
-----------------------
The user can:

 1. Collect the data - 
	 - by reading from a file (obtained from the GKG Exporter).
	 - by querying the GDELT's GKG table hosted on BigQuery.
 2. Pre-process the collected data: The data collected needs to be cleaned and processed before it can be used for analysis. The package has several functions to do the job.

 3. Storage of the data.


----------


Installation
----------------

 1. Download the wheel file (gydelt-1.0-py2.py3-none-any.whl)
 2. Run the command  -
	```
	pip install gydelt-1.0-py2.py3-none-any.whl
	```
> **Requirements: **
> You need Python (2.x or 3.x) to run gydelt (Python 3.x is recommended).
> Also, if you want to query the data from Google's BigQuery, proper authentication will be required in order to do the job. 


----------


Tutorial
------------

A notebook that shows how to use the package and also, a sample use-case

[Sample Notebook]()
