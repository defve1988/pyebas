from .files_io import *
from .query import *

class ValueIndex:
   def __init__(self):      
      # bad qc values from EBAS website      
      self.meta={"ebas":0, "no_ebas":1, 0:"ebas", 1:"no_ebas"}
      self.matrix={}
      self.unit={}
      self.res_code={}
      self.component={}
      self.site={}
   
   def create_value_index(self, site_index):
      print("creating value index...")
      matrix = Query.summary_attr(site_index, "matrix")
      unit = Query.summary_attr(site_index, "unit")
      res_code = Query.summary_attr(site_index, "res_code")
      component = Query.summary_attr(site_index, "component")
      site = list(site_index.keys())
      
      self.update_value_index("matrix", matrix)
      self.update_value_index("unit", unit)
      self.update_value_index("res_code", res_code)
      self.update_value_index("component", component)
      self.update_value_index("site", site)
      
      self.value_index = {
         "matrix":self.matrix,
         "unit":self.unit,
         "res_code":self.res_code,
         "component":self.component,
         "site":self.site,
         "meta": self.meta
      }
      
   def update_site_index(self, site_index):
      """
      update site index with value index (convert string to number)
      """
      for site in site_index.keys():
         for f in site_index[site]["files"].keys():
            for index, content in enumerate(site_index[site]["files"][f]["contents"]):
               site_index[site]["files"][f]["contents"][index]["site"] = self.value2index("site", content["site"])
               site_index[site]["files"][f]["contents"][index]["matrix"] = self.value2index("matrix", content["matrix"])
               site_index[site]["files"][f]["contents"][index]["component"] = self.value2index("component", content["component"])
               site_index[site]["files"][f]["contents"][index]["unit"]= self.value2index("unit", content["unit"])
               site_index[site]["files"][f]["contents"][index]["res_code"] = self.value2index("res_code", content["res_code"])
               site_index[site]["files"][f]["contents"][index]["meta"] = self.value2index("meta", content["meta"])

      return site_index
   
   def update_value_index(self, attr_name, vals):
      temp = {}
      for index, val in enumerate(vals):
         temp[index]= val
         temp[val]= index
      
      setattr(self, attr_name, temp)
      
   def value2index(self, attr, val):
      return self.value_index[attr][val]