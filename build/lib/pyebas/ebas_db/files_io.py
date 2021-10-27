from ..utilities import *
import pickle
import lzma
import json
import os
import numpy as np
import xarray as xr

bad_qc = [459,460,471,530,533,540,549,565,566,567,568,591,599,635,658,659,663,664,666,669,677,682,683,684,685,686,687,699,783,890,980,999]


def load_files(files):
   """this method opens ebas data files
   
   Args:
         files (dict): {"path":"", "name":""}   
   Returns:
         (tuple): (db_index, db)    
   """
   db_index = {}
   db = {}
   res = run_mp(load_file, files)
         
   # combine all the data
   for r in res:
      db_index[r["name"]] = r["data"]["content_index"]
      r["data"].pop("content_index")
      db[r["name"]] = r["data"]

   
   return db_index, db

def load_file(file):
   """this method opens one '.xz', '.json', and python pickle files

   Args:
         file (dict): {"name":"", "path":""}

   Returns:
         dict: {"name":"", "data":""}
   """
   
   file_path = file["path"]
   
   if file_path.endswith("xz"):
      with lzma.open(file_path, "rb") as pickle_file:
         res = pickle.load(pickle_file)
   elif file_path.endswith("json"):
      with open(file_path,"r") as json_file:
         res = json.load(json_file)
   else:
      with open(file_path, "rb") as pickle_file:
         res = pickle.load(pickle_file)
   
   return {"name":file["name"], "data":res}

def load_value(file_path):
   if file_path.endswith("xz"):
      with lzma.open(file_path, "rb") as pickle_file:
         res = pickle.load(pickle_file)
   elif file_path.endswith("json"):
      with open(file_path,"r") as json_file:
         res = json.load(json_file)
   else:
      with open(file_path, "rb") as pickle_file:
         res = pickle.load(pickle_file)
   
   return res

def dump_value(var, dir, file_name, dump): 
   f_name = f"{file_name}.{dump}"
   print(f"Dumping data to to '{f_name}'...")      
   if dump =="xz":
      with lzma.open(os.path.join(dir, f_name), "wb") as pickle_file:
         pickle.dump(var, pickle_file)
   elif dump=="p":
      with open(os.path.join(dir, f_name),"wb") as f:
         pickle.dump(var, f)  
   else:
      with open(os.path.join(dir, f_name),"w") as f:
         json.dump(var, f, indent=4,  sort_keys=True, default=str)
         
def import_site_data(args):
   """
   {
      content_index: {
         id1:{
            st, ed, comp, var, matrix, res_code, units
         } ,
         id2:{
            }
      ,...
      }
      id1:(df: st, ed, val_qc)
      id2:(df: st, ed, val_qc)
   }
   """
   raw_dir, dump_dir, dump, files = args
   res = { "content_index" :{} }
   id_count = 0
   for file in files.keys():
      try:
         site_id = file.split(".")[0]
         ebas = xr.open_dataset(os.path.join(raw_dir, file))
         for content in files[file]["contents"]:
            res["content_index"][id_count] = {
                  "st": content["st"],
                  "ed": content["ed"],
                  "component":content["component"],
                  "matrix": content["matrix"],
                  "res_code": content["res_code"],
                  "unit": content["unit"],
                  "var": content["var"],
                  "stat": content["stat"],
                  "file":file
            }
            # temp = pd.DataFrame()
            st = ebas["time_bnds"].data[:,0]
            ed = ebas["time_bnds"].data[:,1]
            ts = np.array([st,ed]).T
            
            # get var value and qc, the values can be updated for several times, so additional dimensions may be applied
            val = ebas[content["var"]].data
            while len(val.shape)>1:
               val = val[-1,:]  
            qc = ebas[content["var"]+"_qc"].data
            while len(qc.shape)>1:
               qc = qc[-1,:]
            
            # filter value with qc values
            val[np.isin(qc, bad_qc)]=None
            
            val = np.array([val]).T
            
            res[id_count] = {"ts": ts, "val": val}
            id_count+=1
         
      except Exception as e:
         print(e, file)  
   
   if dump=="xz":   
      with lzma.open(os.path.join(dump_dir, f"{site_id}.xz"), "wb") as pickle_file:
         pickle.dump(res, pickle_file)
   else:
      with open(os.path.join(dump_dir, f"{site_id}.p"), "wb") as pickle_file:
         pickle.dump(res, pickle_file)