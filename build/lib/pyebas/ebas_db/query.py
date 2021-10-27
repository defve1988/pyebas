import numpy as np
import pandas as pd
import itertools
from tqdm import tqdm

class Query:
   @staticmethod
   def db_overview(site_index):
      country = list(set(map(lambda x: site_index[x]["country"], site_index)))
      matrix = list(map(lambda x: list(site_index[x]["matrix"].keys()), site_index))
      matrix = list(set(itertools.chain(*matrix)))

      components = list(map(lambda x: list(site_index[x]["components"].keys()), site_index))
      components = list(set(itertools.chain(*matrix)))
           
      print(f"{len(site_index.keys()):<10} sites included in current database.")
      print(f"{len(components):<10} components included in current database.")
      print(f"{len(matrix):<10} matrix included in current database.")
      print(f"{len(country):<10} country included in current database.")
      
   # @staticmethod
   # def summary(site_index, site_keys=None):
   #    # generate summary for all sites or selected sites
   #    if site_keys is None:
   #       site_keys = site_index.keys()
         
   #    res = {}      
   #    for k in site_keys:
   #       res["site"]={
   #          "country":site_index[k]["country"],
   #          "land_use":site_index[k]["land_use"],
   #          "station_setting":site_index[k]["station_setting"],
   #          "name":site_index[k]["name"],
   #          "id":site_index[k]["id"],
   #          "lat":site_index[k]["lat"],
   #          "lon":site_index[k]["lon"],
   #          "alt":site_index[k]["alt"],
   #          "components": Query.summary_attr(site_index, "component", site_keys=[k]),
   #          "matrix": Query.summary_attr(site_index, "matrix", site_keys=[k]),
   #          "st": Query.summary_attr(site_index, "st", site_keys=[k]),
   #          "ed": Query.summary_attr(site_index, "ed", site_keys=[k]),
   #          }
      
   #    return res
   
   @staticmethod
   def summary_attr(site_index, attr, site_keys=None):
      # attr can be anything in contents: matrix, res_code, unit, var, component
      if site_keys is None:
         site_keys = site_index.keys()
      res = []
      for site in site_keys:
         files = site_index[site]["files"]
         for file in files.keys():
            contents = files[file]["contents"]
            for c in contents:
               res.append(c[attr])
      res = list(set(res))
      res.sort()
      return res
   
   @staticmethod
   def query(site_index, db_index, condition, value_index):
      selected = {}
      condition_key = list(condition.keys())
      site_selector_key = ["id", "name", "land_use", "station_setting","country"]
      site_selector_key = list(set(site_selector_key) & set(condition_key))
      # if "id" in site_selector_key:
      #    condition["id"] = list(map(lambda x: value_index["site"][x], condition["id"]))
            
      measure_selector_key = ["component", "matrix","stat"]
      measure_selector_key = list(set(measure_selector_key) & set(condition_key))
      
      if "component" in measure_selector_key:
         cond_temp = []
         for c in condition["component"]:
            if c in value_index["component"].keys():
               cond_temp.append(value_index["component"][c])
         condition["component"] = cond_temp
         # condition["component"] = list(map(lambda x: value_index["component"][x], condition["component"]))
      if "matrix" in measure_selector_key:
         cond_temp = []
         for c in condition["matrix"]:
            if c in value_index["matrix"].keys():
               cond_temp.append(value_index["matrix"][c])
         condition["matrix"] = cond_temp
         # condition["matrix"] = list(map(lambda x: value_index["matrix"][x], condition["matrix"]))
  

      time_selector_key =  ["st", "ed"]
      time_selector_key = list(set(time_selector_key) & set(condition_key))
      
      time_selector = {}
      for k in time_selector_key:
         time_selector[k] = condition[k]
      for site_id in tqdm(site_index.keys(), desc="seraching..."):
         select =True
         if len(site_selector_key)>0:
            for key in site_selector_key:
               if site_index[site_id][key] not in condition[key]:
                  select=False
                  break
               
         # if the site is not the target, examing files are not necessary.
         if not select:
            continue
         
         files = []
         for file_id in db_index[site_id].keys():
            select_file = True
            content = db_index[site_id][file_id]
            if "st" in site_selector_key:
               if content["ed"]<condition["st"]:
                  select_file = False
            if "ed" in site_selector_key:
               if content["st"]>condition["ed"]:
                  select_file = False
            for k in measure_selector_key:
               if content[k] not in condition[k]:
                  select_file = False
                  break
            if select_file:
               files.append(file_id)
         
         if len(files)>0:
            selected[site_id] = files
      
      return (selected, time_selector)
   
   @staticmethod
   def get_df(db, db_index, query_res, use_number_index, value_index):
      print("Gathering data to dataframe...")
      selected, time_selector = query_res
      res_ts = []
      res_val = []
      
      for site in tqdm(selected.keys()):
         site_id = site
         for file in selected[site_id]:
            header = db_index[site_id][file]
            ts = db[site_id][file]["ts"]
            val = db[site_id][file]["val"]
            
            if len(time_selector)>0:
               if "st" in time_selector.keys():
                  st = time_selector["st"]
                  index = ts[:,0]>= st
                  ts = ts[index]
                  val = val[index]
               if "ed" in time_selector.keys():
                  ed = time_selector["ed"]
                  index = ts[:,1]<=ed
                  ts = ts[index]
                  val = val[index]           
            
            infor = np.empty((ts.shape[0], 4))
            infor[:,0] = value_index["site"][site_id]
            infor[:,1] = header["component"]
            infor[:,2] = header["unit"]
            infor[:,3] = header["matrix"]
                        
            val = np.hstack((val,infor))
            
            res_ts.append(ts)
            res_val.append(val)
      
      df = None
      if len(res_ts)>0:
         res_ts = np.vstack(tuple(res_ts))
         res_val = np.vstack(tuple(res_val))
         
         df1 = pd.DataFrame(res_ts, columns=["st", "ed"])
         df2 = pd.DataFrame(res_val, columns=["val", "site","component","unit","matrix"])
         df = pd.concat([df1,df2],axis=1)

         if not use_number_index:
            df["site"] =df["site"].map(value_index["site"])
            df["component"] =df["component"].map(value_index["component"])
            df["unit"] =df["unit"].map(value_index["unit"])
            df["matrix"] =df["matrix"].map(value_index["matrix"])
         
      return df