import os
import xarray as xr
from tqdm import tqdm
import numpy as np
import pandas as pd
import time
import json

class csvExporter:
   def __init__(self, loc=None):
      if loc is None:
         cwd = os.getcwd()
         self.loc = os.path.join(cwd, 'ebas','raw_data')
         self.csv_loc = os.path.join(cwd, 'ebas')
      else:
         self.loc = os.path.join(loc,'raw_data')
         self.csv_loc = loc
      if not os.path.exists(self.loc):
         os.makedirs(self.loc)
         print(f"Make data folder {self.loc}...") 
         
   def export_csv(self, file_name=None):
      res = pd.DataFrame()
      
      files = os.listdir(self.loc)
      nc_files = list(filter(lambda x: x.endswith("nc"), files))
      for file in tqdm(nc_files, desc="Processing files..."):
         ebas = xr.open_dataset(os.path.join(self.loc, file))
         ebas_metadata = ebas.ebas_metadata
         ebas_metadata = json.loads(ebas_metadata)
         
         # file time series
         st = np.array(ebas["time_bnds"].data[:,0])
         ed = np.array(ebas["time_bnds"].data[:,1])
         
         # file site information
         site_id = ebas_metadata["Station code"]
         # site_name = ebas_metadata["Station name"]
         try:
            alt = ebas_metadata["Station altitude"]                           
         except:
            alt = ""
         try:           
            lat = ebas_metadata["Station latitude"]                           
         except:
            lat = ""
         try:            
            lon = ebas_metadata["Station longitude"]               
         except:
            lon = ""      
         
         # file variable information
         vars = list(ebas.data_vars.keys())
         vars = list(filter(lambda x: not x.endswith("_qc") and not x.endswith("_ebasmetadata"), vars))
         vars.remove("time_bnds")
         vars.remove("metadata_time_bnds")   
         for v in vars:
            temp = ebas[v+"_ebasmetadata"].data.tolist()[-1]
            while isinstance(temp, list):
               temp = temp[-1]
            temp = json.loads(temp)
            
            if "Matrix" in temp.keys():
               matrix = temp["Matrix"],
               unit = temp["Unit"],
               stat = temp["Statistics"],
               component = temp["Component"],
            elif "ebas_matrix" in temp.keys():
               matrix = temp["ebas_matrix"],
               unit = temp["ebas_unit"],
               stat = temp["ebas_statistics"],
               component = temp["ebas_component"],
            
            # get var value and qc, the values can be updated for several times, so additional dimensions may be applied       
            val = ebas[v].data
            while len(val.shape)>1:
               val = val[-1,:]  
            qc = ebas[v+"_qc"].data
            while len(qc.shape)>1:
               qc = qc[-1,:]
            
            temp_pd = pd.DataFrame({"st":st, "ed":ed})
            
            temp_pd["site"] = site_id
            # temp_pd["name"] = site_name
            temp_pd["alt"] = alt
            temp_pd["lat"] = lat
            temp_pd["lon"] = lon
            temp_pd["val"] = val
            temp_pd["qc"] = qc
            temp_pd["unit"] = unit[0]
            temp_pd["matrix"] = matrix[0]
            temp_pd["stat"] = stat[0]
            temp_pd["component"] = component[0]    
            
            res = pd.concat([res,temp_pd], ignore_index= True)   
            
      print("Exporting...")
      if file_name is None:
         t= time.strftime("%Y%m%d%H%M")
         file_name = f"pyebas_{t}.csv"
      out_file = os.path.join(self.csv_loc, file_name)
      res.to_csv(out_file, index=False)
      print(f"Data has been exported to {out_file}.")
   
   