import os
from tqdm import tqdm
from argparse import Namespace
from ..utilities import *

class EbasDownloader:
   def __init__(self, loc=None, url=None, ftp=None, bs4_selector=None):
      """init

      Args:
          loc (str, optional): path to save raw data. Defaults to None.
          url (str, optional): url for ebas file list. Defaults to None.
          ftp (str, optional): url for ebas ftp server. Defaults to None.
          bs4_selector (str, optional): selector for bs4. Defaults to None.
      """
      self.loc, self.url, self.ftp, self.bs4_selector = loc, url, ftp, bs4_selector
      if loc is None:
         cwd = os.getcwd()
         self.loc = os.path.join(cwd, 'ebas', 'raw_data')
      else:
         self.loc = os.path.join(loc, 'raw_data')
         
      if not os.path.exists(self.loc):
         os.makedirs(self.loc)
         print(f"Make data folder {self.loc}...")
         
      if ftp is None:
         self.ftp = 'https://thredds.nilu.no/thredds/fileServer/ebas/'
      if url is None:
         self.url = 'https://thredds.nilu.no/thredds/catalog/ebas/catalog.html'
      if bs4_selector is None:
         self.bs4_selector = "tr > td > a > tt"
   
   def get_raw_files(self, conditions=None, download=False):
      """update local files using current ebas files.

      Args:
          conditions (dict, optional): files selection conditions. Defaults to None.
          download (bool, optional): whether performs download. Defaults to False.
      """
      loc_files = self.__get_loc_files()
      ftp_files = self.__get_ebas_files()
      
      # remove files no longer exists on ftp server
      del_files = list(filter(lambda x: x not in ftp_files, loc_files))
      print(f"{len(del_files)} files need to be deleted...")
      if len(del_files)>0:
         self.__del_files(del_files)
      
      if conditions is not None:
         if isinstance(conditions, dict):
            conditions = Namespace(**conditions)
         ftp_files = self.__select_files(ftp_files, conditions)
      new_files = list(filter(lambda x: x not in loc_files, ftp_files))
      
      print(f"{len(new_files)} files need to be downloaded...")
      
      if not download and len(new_files)>0:
         p = input("View file names? (y/n) ")
         if p.lower() in ['yes', 'y', 'Y', 'Yes']:
            if len(new_files)>0:
               print("Files will be downloaded:")
               for f in new_files:
                  print(f)
            if len(del_files)>0:
               print("Files will be deleted:")
               for f in del_files:
                  print(f)
         download = input("Start downloading? (y/n) ")
         download = True if download.lower() in ['yes', 'y', 'Y', 'Yes'] else False
         
      if download:
         self.__download_files(new_files) # download new files
         
   def __download_files(self, files):
      ftp_list = []
      for (_, f) in enumerate(files):
         ftp_list.append((self.ftp+"/"+f, self.loc))
      if len(ftp_list)>0:
         print("Start downloading files...")
         run_mp_async(ftp_get_file, ftp_list)
         print("\nDownload completed.")

   def __del_files(self, files):
      for f in files:
         os.remove(os.path.join(self.loc, f))
      print("Old files have been removed.")
     
   def __select_files(self, files, conditions):
      """select files with args from files list

      Args:
          files (list): online ebas file lists
          conditions (Namespace): selection conditions
          may include start_year, end_year, sites, matrix, componets
      """
      
      ma = []
      com = []
      
      selected = []
      for f in tqdm(files, desc="selecting ftp files..."):
         temp = f.split('.')
         site = temp[0]
         st = int(temp[1][0:4])
         ed = int(temp[2][0:4])
         matrix = temp[5]
         component = temp[4]
         
         ma.append(matrix)
         com.append(component)
         
         if conditions.start_year is not None:
            if int(conditions.start_year) > ed:
               continue
         if conditions.end_year is not None:
            if int(conditions.end_year) < st:
               continue
         if conditions.site is not None:
            if site not in conditions.site:
               continue
         if conditions.matrix is not None:
            if matrix not in conditions.matrix:
               continue
         if conditions.components is not None:
            if component not in conditions.components:
               continue
         
         selected.append(f)
      
      # print(sorted(set(ma)))
      # print(sorted(set(com)))
      
      return selected
   
   def __get_ebas_files(self):
      print("Requesting data from ebas sever...")
      files = bs4_get(self.url, self.bs4_selector)
      # the first line is "Ebas", not a file
      print(f"{len(files)-1} files found on ftp server.")
      return files[1:]
   
   def __get_loc_files(self):
      nc_files =[]
      files = os.listdir(self.loc)
      nc_files = list(filter(lambda x: x.endswith("nc"), files))      
      print(f"{len(nc_files)} raw data (*.nc) files have been downloaded.")
      
      return nc_files
 