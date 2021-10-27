import os 
import xarray as xr
import json
import numpy as np

from ..utilities import *
from .files_io import *


class SiteIndex():
   def __init__(self, raw_dir, dump_dir, dump='xz', detailed=True):
      """site index

      Args:
          raw_dir (str): raw data path
          detailed (bool, optional): whether save detail descriptions from .nc file to database. Defaults to True.

      SiteIndex example:
         {
           id: site id,
           name: site name,
           country: Armenia,
           land_use: land use type,
           station_setting: station type,
           alt: 2080.0 m,
           lat: 40.38444444,
           lon: 44.260583333,
           components:{
              <component name>:{
                 st: starting date, datetime,
                 ed: ending date, datetime
              }
           }
           files: {
              <file_name>:{
                 content:[
                    <dict of each component in this file>, eg:  
                        {
                           res_code: resolution,
                           matrix: matrix,
                           unit: unit,
                           component: component name,
                           stat: statistics type,
                           st: starting date, datetime,
                           ed: ending date, datetime,
                           site: site id,
                           var: var name in nc file,
                        }
                 ],
              }
           }     
          }
      """
      self.raw_dir = raw_dir
      self.dump = dump
      self.detailed = detailed
      self.site_index = None
      self.dump_dir = dump_dir
      
      super(SiteIndex, self).__init__()
   
   def create_site_index(self):
      print("Gathering site information...")
      # filter out non ".nc" files
      raw_data = os.listdir(self.raw_dir)
      files = list(filter(lambda x: x.endswith('nc'), raw_data))
      self.site_index = run_mp(self.get_file_indexing, files, self.combine_file_index)
      
      print(f"Collected site number: {len(self.site_index.keys())}")
      # analysis bad files
      bad =[]
      for k in self.site_index.keys():
         if k.startswith(self.raw_dir):
            bad.append(k)

      if len(bad)>0:
         print(f"Bad files :{len(bad)}")
         for b in bad:
            print(b)
      else:
         print("No bad files were found.")
      
   def get_file_indexing(self, file_name):
      """gathering indexing information from one .nc file

      Args:
          file_name (str): .nc file name

      Returns:
          dict: indexing information, similar as SiteIndex
      """
      try:
         # get site information
         ebas = xr.open_dataset(os.path.join(self.raw_dir, file_name))
         ebas_metadata = ebas.ebas_metadata
         ebas_metadata = json.loads(ebas_metadata)
            
         site = {
                "id": ebas_metadata["Station code"],
                "name": ebas_metadata["Station name"],
                "country": code2country(ebas_metadata["Station code"][0:2]),
                "land_use":None,
                "station_setting":None,
                "alt":None,
                "lat":None,
                "lon":None,
                "files":{},
               #  "var_content":[]
            }
         try:
            site["land_use"] = ebas_metadata["Station land use"]           
         except:
            pass
         try:
            site["station_setting"] = ebas_metadata["Station setting"]         
         except:
            pass
         try:
            site["alt"] = ebas_metadata["Station altitude"]                           
         except:
            pass
         try:           
            site["lat"] = ebas_metadata["Station latitude"]                           
         except:
            pass
         try:            
            site["lon"] = ebas_metadata["Station longitude"]               
         except:
            pass
         
         # get var content
         vars = list(ebas.data_vars.keys())
         vars = list(filter(lambda x: not x.endswith("_qc") and not x.endswith("_ebasmetadata"), vars))
         vars.remove("time_bnds")
         vars.remove("metadata_time_bnds")
         
         var_content = []
         for v in vars:
            temp = ebas[v+"_ebasmetadata"].data.tolist()[-1]
            
            while isinstance(temp, list):
               temp = temp[-1]
               
            temp = json.loads(temp)
            
            if "Matrix" in temp.keys():
               content ={
                  "res_code": ebas_metadata["Resolution code"],
                  "matrix": temp["Matrix"],
                  "unit":temp["Unit"],
                  "meta": "no_ebas", 
                  
                  "var":v,
                  "site": ebas_metadata["Station code"],
                  "stat": temp["Statistics"],
                  "component":temp["Component"],
                  "st":ebas["time_bnds"][0,0].values,
                  "ed":ebas["time_bnds"][-1,1].values,
               }
            elif "ebas_matrix" in temp.keys():
               content ={
                  # "res": res_code_index[ebas_metadata["Resolution code"]],
                  # "matrix": matrix_index[temp["ebas_matrix"]],
                  # "unit":units_index[temp["ebas_unit"]],
                  # "meta": meta_index["no_ebas"],
                  
                  "res_code": ebas_metadata["Resolution code"],
                  "matrix": temp["ebas_matrix"],
                  "unit":temp["ebas_unit"],
                  "meta": "no_ebas",
                  
                  "var":v,
                  "site": ebas_metadata["Station code"],
                  "stat": temp["ebas_statistics"],
                  "component":temp["ebas_component"],
                  "st":ebas["time_bnds"][0,0].values,
                  "ed":ebas["time_bnds"][-1,1].values,
               }
               
            var_content.append(content)
            
         # get attr information
         attr_content={}
         if self.detailed:
            attrs = ebas.attrs
            attr_content ={}
            for a in attrs:
               temp = getattr(ebas,a)
               if isinstance(temp, np.ndarray):
                  temp= temp.tolist()
               attr_content[a] = temp
         
         site["files"] = {file_name:{"contents": var_content, "detail_attrs": attr_content}}
                       
         return {site["id"]: site}
      
      except Exception as e:
         print(e)
         print(file_name)
         return {file_name: {
                "id": "",
                "name": "",
                "land_use": "",
                "station_setting": "",
                "lat": "",
                "lon": "",
                "alt": "",
                "error":str(e),
            }}
         
   def combine_file_index(self, list_dict_res):
      res = {}
      for d in list_dict_res:
         id = list(d.keys())[0]
         if id not in res.keys():
            res[id] = d[id]
            # will add stats for how many files this site has
            res[id]["file_num"] =1
            res[id]["components"]={}
            res[id]["matrix"]={}
         else:
            res[id]["file_num"] +=1
            res[id]["files"].update(d[id]["files"])
         
         # create a component indexing dict
         for f in d[id]["files"].keys():
            contents = d[id]["files"][f]["contents"]
            for c in contents:
               if c["component"] not in res[id]["components"].keys():
                  res[id]["components"][c["component"]] = [f]
               else:
                  res[id]["components"][c["component"]].append(f)
               
               if c["matrix"] not in res[id]["matrix"].keys():
                  res[id]["matrix"][c["matrix"]] = [f]
               else:
                  res[id]["matrix"][c["matrix"]].append(f)
                               
      return res