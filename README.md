# pyebas
pyebas is created for an easy-access to open-source air pollutant data from [EBAS database](http://ebas-data.nilu.no/default.aspx) via their [FTP server](https://thredds.nilu.no/thredds/catalog/ebas/catalog.html). EBAS database collects mainly from EU air pollutant monitoring programs.

pyebas provides can both download files from EBAS database and created local database for further usage. The downloaded raw EBAS files (.nc file) can be exported to .csv files. The local pyebas database converts ~25GB EBAS raw data to ~800MB local files. Users can access and query data through local database.



1. Import pyebas

   ~~~shell
   pip3 install pyebas
   ~~~

   ~~~python
   from pyebas import *
   ~~~

2. Download EBAS data (.nc files)

   ~~~python
   # set selection conditions
   # if you need the whole EBAS database, set conditions as None
   conditions = {
       "start_year": 1990,
       "end_year": 2021,
       "site": ['ES0010R', 'ES0011R'],
       "matrix": ['air'],
       "components": ['NOx'],
   }
   # set local stroage path
   db_dir = r'ebas_db'
   downloader = EbasDownloader(loc=db_dir)
   # download requires multiprocessing, error may occurs because of multiprocessing
   # use command line or Jupyter Notebook to prevent errors
   downloader.get_raw_files(conditions=conditions, download=True)
   ~~~

3. Export to .csv file

   ~~~python
   # export all the downloaded .nc files in the output path to .csv 
   # important: .csv file might be very large.
   csv_exporter = csvExporter(loc=db_dir)
   csv_exporter.export_csv('export.csv')
   ~~~

4. Create local database

   ~~~python
   # set local stroage path, must be the same as previous path
   db_dir = r'ebas_db'
   # local database object
   db = EbasDB(dir=db_dir, dump='xz', detailed=True)
   # create/update database with new files
   db.update_db()
   ~~~

5. Open local database

   ~~~python
   # set local stroage path
   db_dir = r'ebas_db'
   # local database object
   db = EbasDB(dir=db_dir, dump='xz', detailed=True)
   # open database if it is created
   db.init_db()
   ~~~

6. Query data from local database as pandas.DataFrame

   ~~~python
   condition = {
       "id":["AM0001R", "EE0009R", 'ES0010R', 'ES0011R'],
       "component":["NOx", "nitrate", "nitric_acid"],
       "matrix":["air", "aerosol"],
       "stat":['arithmetic mean',"median"],
       "st":np.datetime64("1970-01-01"),
       "ed":np.datetime64("2021-10-01"),
       # if you want to include all, just remove the condition
       #"country":["Denmark","France"],
   }
   df = db.query(condition, use_number_indexing=False)
   df.head(20)
   ~~~

7. Access detail information

   ~~~python
   # access information for one site
   db.site_index["ES0011R"]
   db.site_index["ES0011R"]["components"].keys()
   db.site_index["ES0011R"]["files"]
   ~~~

8. Get summary

   ~~~python
   # get summary information
   db.list_sites()
   # possible keys are: "id","name","country","station_setting", "lat", "lon","alt","land_use", "file_num","components"
   db.list_sites(keys=["name","lat","lon"])
   # if components are selected, set list_time=True to see the starting and ending time
   db.list_sites(keys=["name", "components"], list_time=True)
   ~~~

9. Use command line

   Possible arguments, use `pyebas --help` for details and options for `matrix` and `components`: 

   ~~~shell
   pyebas 
   <starting year> 
   <ending year> 
   --mode <csv, db, query> 
   --site <site id, site id> 
   --matrix <matrix type> 
   --components <component name> 
   --output <output path>
   ~~~

   Example 1: download NOx measurements in air of two sites (ES0010R and ES0011R) from 2019 to 2021, the results will be exported as .csv file.

   ~~~shell
   pyebas 2019 2021 --mode csv --site ES0010R ES0011R --matrix air --components NOx --output .\simple_csv
   ~~~

   Example 2: download all measurements from 2019 to 2021, and stored in local database. 

   ~~~shell
   pyebas 2019 2021 --mode db --output .\ebas
   ~~~

   Start querying with the created local database (you need enter conditions through terminal later, and the results can be exported to .csv files).

   ~~~shell
   python main.py 2019 2021 --mode query --out .\ebas
   ~~~

   

