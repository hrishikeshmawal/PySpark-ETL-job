# PySpark-ETL-job

The input data is taken from Met Museum collection in New York. We want to get deep 
into it and find insights which interests us.

The dataset is accessible on below link:


https://github.com/metmuseum/openaccess/

Note: The above file is stored through Git Large File Storage (LFS), and hence might not be immediately consumable after a git clone. In that case, please 
install Git LFS, or use the Git Web UI and download the raw file. 

Then the following steps will be followed:

*  Data consumption
*  Data Pre-Processing
*  Aggregation
*  Storage

Following questions were answeredand the output tables can be stored as csv files or in any database

*  Get the number of artworks per individual country
*  Get the number of artists per country
*  Average height, width, and length per country
*  Collect a unique list of constituent ids per country
