import os
import xarray as xr
from .site_index import *
from .value_index import *
from .query import *
from .files_io import *

class EbasDB(SiteIndex, ValueIndex):
   def __init__(self, dir=None, dump='xz', **agrs):
      """EBAS database

      Args:
          dir (str, optional): path to database. Defaults to None.
          dump (str, optional): dump file type, 'xz', 'p', or 'json'. Defaults to 'xz'.
                                note: json file can be only used for exporting, 
                                      it can't be used as indexing file in databas. 
      """
      self.dump = dump
      self.db_dir = None
      self.dump_dir = None
      self.raw_dir = None
      # index of sites, including site information, and file content
      self.site_index = None
      self.value_index = None
      # inverse indexing of file content in site_index
      self.db_index={}
      self.db={}
      self.selected = {}
            
      self.bad_qc = [459,460,471,530,533,540,549,565,566,567,568,591,599,635,658,659,663,664,666,669,677,682,683,684,685,686,687,699,783,890,980,999]
      self.__make_dir(dir)
      super(EbasDB, self).__init__(raw_dir=self.raw_dir, 
                                   dump_dir=self.dump_dir,
                                   dump=dump,
                                   **agrs)
      
   def init_db(self):
      print("init database...")     
      print("Load value index...")
      file_path = os.path.join(self.db_dir, f"value_index.{self.dump}")
      self.value_index = load_value(file_path) 
      
      print("Load site index...")
      file_path = os.path.join(self.db_dir, f"site_index.{self.dump}")
      self.site_index = load_value(file_path)
      
      print("Load site data...")    
      files = []
      suffix = '.xz' if self.dump=="xz" else '.p'
      for site in self.site_index.keys():
         temp = {"path": os.path.join(self.dump_dir, site+suffix),
               "name": site}
         files.append(temp)
      self.db_index, self.db = load_files(files)
      Query.db_overview(self.site_index)
      print("Database is loaded.")
      
   def update_db(self):
      # create site index
      self.create_site_index()
      # create value index
      self.create_value_index(self.site_index)
      dump_value(self.value_index, self.db_dir, "value_index", self.dump)
      # update site index with value index
      self.site_index = self.update_site_index(self.site_index)
      dump_value(self.site_index, self.db_dir, "site_index", self.dump)
      # import data
      self.__import_data()
   
   
   def query(self, query_dict, use_number_indexing=True):
      query_res = Query.query(self.site_index, self.db_index, query_dict, self.value_index)
      df = Query.get_df(self.db, self.db_index, query_res, use_number_indexing, self.value_index)
      return df
   
   def __import_data(self):
      arg_list = []
      for k in self.site_index.keys():
         arg_list.append((self.raw_dir, self.dump_dir, self.dump, self.site_index[k]["files"]))
      
      print("Importing datafile for each site...")
      utilities.run_mp(import_site_data, arg_list)


   def __make_dir(self, dir):
      if dir is None:
         cwd = os.getcwd()
         self.db_dir = os.path.join(cwd, 'ebas')
         self.dump_dir = os.path.join(cwd, 'ebas', 'dumps')
         self.raw_dir = os.path.join(cwd, 'ebas', 'raw_data')
      else:
         self.db_dir = dir
         self.dump_dir = os.path.join(dir, 'dumps')
         self.raw_dir = os.path.join(dir, 'raw_data')
         
      if not os.path.exists(self.db_dir):
         os.makedirs(self.db_dir)
         print(f"Make data folder {self.db_dir}...")
      if not os.path.exists(self.dump_dir):
         os.makedirs(self.dump_dir)
         print(f"Make data folder {self.dump_dir}...")
      if not os.path.exists(self.raw_dir):
         os.makedirs(self.raw_dir)
         print(f"Make data folder {self.raw_dir}...")
