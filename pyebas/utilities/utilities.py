import concurrent.futures
import multiprocessing
from tqdm import tqdm
import csv
import pycountry
import wget
import os


from bs4 import BeautifulSoup
import requests



# simple web scrap for sth
def bs4_get(url, selector, tags=None):
   # tags = none, will return a list of target text
   # tags= [], will return a list of dict [{tag1:content, tag2: content}, ...]
   wb_res = requests.get(url)
   if wb_res.status_code !=200:
      raise ValueError("Connection error.")

   soup = BeautifulSoup(wb_res.text, 'lxml')
   
   selected = soup.select(selector)
   res = []
   for i in selected:
      if tags is None:
         res.append(i.text)
      else:
         temp={}
         for t in tags:
            temp[t] = i.get(t)
         
         res.append(temp)
      
   return res

def ftp_get_file(ftp, out_path):
   try:
      wget.download(ftp, out=out_path)
   except Exception as e:
      print(e)
      with open(os.path.join(out_path, 'ftp_error.txt'),'a') as f:
         f.write(ftp+'\n')

def run_mp_async(map_func, arg_list,num_cores=None):
   if num_cores is None:
      num_cores = multiprocessing.cpu_count()
      num_cores = len(arg_list) if len(arg_list)<num_cores else num_cores
   print(f"Using {num_cores} threads...")
   
   pool = multiprocessing.Pool(num_cores)
   for arg in arg_list:
      pool.apply_async(map_func,  args=arg) 
   pool.close()
   pool.join()

def run_mp(map_func, arg_list, combine_func=None, num_cores=None):
   if num_cores is None:
      num_cores = multiprocessing.cpu_count()
      num_cores = len(arg_list) if len(arg_list)<num_cores else num_cores
   print(f"Using {num_cores} threads...")
   
   with concurrent.futures.ProcessPoolExecutor(max_workers=num_cores) as pool:
      with tqdm(total=len(arg_list)) as progress:
         futures = []
         for args in arg_list:
               future = pool.submit(map_func, args)
               future.add_done_callback(lambda p: progress.update())
               futures.append(future)

         results = []
         
         for future in futures:
               result = future.result()
               results.append(result)
   
   if combine_func is not None:
      return combine_func(results)
   else:
      return results   
         

def list2csv(data, file_name, header=None, single_col=True):
   with open(file_name, 'w', newline='', encoding="utf-8") as f:
    write = csv.writer(f)
    if header is not None:
      write.writerow(header)
    if single_col:
      for item in data:
          write.writerow([item])
    else:
       write.writerows(data)
   
   print(f"Data is written to {file_name}.")
   

# convert country name and code 
def country2code(country):
   return pycountry.countries.search_fuzzy(country)[0].alpha_2
def code2country(country_code):
   return pycountry.countries.get(alpha_2=country_code).name


