import argparse
import numpy as np
import time
from .ebas_downloader import *
from .ebas_db import *


def main():
   description = """
-----------------------------------------------------------------------------------------------------------
Download EBAS data with specified arguments:
   1. starting year
   2. ending year
   3. site code
   4. matrix
   5. components
If no values are provide, all data will be downloaded.
The result will be exported as .csv file.

Available matrix:
'aerosol', 'aerosol_humidified', 'air', 'air+aerosol', 'air+pm10', 
'dried_moss', 'instrument', 'met', 'pm1', 'pm10', 'pm10_humidified', 
'pm10_non_volatile', 'pm10_pm1', 'pm10_pm25', 'pm1_humidified', 
'pm1_non_refractory', 'pm25', 'pm_eq110', 'pm_eq165', 'pm_eq25', 'pm_eq35', 
'pm_eq50', 'pm_eq75', 'precip', 'precip+dry_dep', 'precip_tot', 'wetdep'

Available components:
'1-3-butadiene', '1-butene', '1-methylnaphthalene', '1-methylphenanthrene', 
'2-2-4-trimethylpentane', '2-2-dimethylbutane', '2-2-dimethylpropane', '2-butenal', 
'2-methylbenzaldehyde', '2-methylbutane', '2-methylpentane', '2-methylpropane', 
'2-methylpropenal', '2-methylpropene', '2-oxopropanal', '2-propenal', 
'3-buten-2-one', '3-methylpentane', 'BDE_154', 'BDE_209', 'FTS_6-2', 'HCB', 
'NOx', 'NOy', 'PCB_114', 'PCB_118', 'PCB_150', 'PCB_153', 'PCB_18', 'PCB_180', 
'PCB_209', 'PCB_28', 'PCB_28+16', 'PCB_99', 'UVA_radiation', 'acenaphthene', 
'acenaphthylene', 'acidity', 'aerosol_absorption_coefficient', 'aerosol_light_backscattering_coefficient', 
'aerosol_light_scattering_coefficient', 'aerosol_optical_depth', 'aldrin', 'alpha_HCH', 
'aluminium', 'ammonia', 'ammonium', 'antimony', 'arsenic', 'barium', 'benz_a_anthracene', 
'benzaldehyde', 'benzene', 'benzo_a_pyrene', 'benzo_b_fluoranthene', 'benzo_bj_fluoranthenes', 
'benzo_bjk_fluoranthenes', 'benzo_k_fluoranthene', 'bromide', 'butanales', 'butanone', 
'cadmium', 'calcium', 'carbon_dioxide', 'carbon_monoxide', 'cesium', 'chloride', 'chromium', 
'chrysene', 'chrysene_triphenylene', 'cloud_condensation_nuclei_number_concentration', 
'cloud_condensation_nuclei_number_size_distribution', 'cobalt', 'conductivity', 'copper', 
'cyclo-hexane', 'dibenzo_ah_anthracene', 'dibenzofuran', 'dimethylsulfide', 'dinitrogen_monoxide', 
'elemental_carbon', 'endosulfan', 'equivalent_black_carbon', 'equivalent_black_carbon_mass', 
'erbium', 'ethanal', 'ethane', 'ethanedial', 'ethene', 'ethylbenzene', 'ethyne', 'fluorantene', 
'fluoride', 'gamma_HCH', 'gaseous_elemental_mercury', 'hexanal', 'hydrochloric_acid', 'hydrogen',
'hygroscopic_growth_factor', 'inden_123cd_pyrene', 'iron', 'lanthanum', 'lead', 'levoglucosan',
'm-p-xylene', 'magnesium', 'manganese', 'mercury', 'methanal', 'methane', 'methanesulfonic_acid', 
'molybdenum', 'n-butane', 'n-heptane', 'n-hexane', 'n-octane', 'n-pentane', 'nickel', 'niobium',
'nitrate', 'nitric_acid', 'nitrogen_dioxide', 'nitrogen_monoxide', 'o-xylene', 'organic_carbon',
'organic_carbon_corrected', 'organic_mass', 'ozone', 'pH', 'particle_number_concentration',
'particle_number_size_distribution', 'pentachlorobenzene', 'pentanal', 'pm10_mass', 'pm10_pm25_mass', 
'pm1_mass', 'pm25_mass', 'potassium', 'pp_DDD', 'pp_DDT', 'precipitation_amount', 
'precipitation_amount_off', 'pressure', 'propanal', 'propane', 'propanone', 'propene', 'propyne', 
'reactive_gaseous_mercury', 'relative_humidity', 'sample_count', 'scandium', 'sodium', 'strontium',
'sulphate_corrected', 'sulphate_total', 'sulphur_dioxide', 'sum_DDT', 'sum_PCB', 
'sum_ammonia_and_ammonium', 'sum_nitric_acid_and_nitrate', 'susp_part_matter', 'temperature', 'thallium', 
'tin', 'titanium', 'toluene', 'total_carbon', 'total_gaseous_mercury', 'trans-2-butene', 'vanadium', 
'voc_complete', 'wind_direction', 'wind_speed', 'zinc'

Important: If you are planning to download all the EBAS data 
           Do not use this command as the output csv file will be extremely large (>30GB).
           Do create EBAS database with pyebas.
           
Example: pyebas 2019 2021 --mode csv --site ES0010R ES0011R --matrix air --components NOx --output .\simple_csv_test
-----------------------------------------------------------------------------------------------------------
   """
   
   parser = argparse.ArgumentParser(prog="Simple download EBAS data",
                                    description=description,
                                    formatter_class=argparse.RawDescriptionHelpFormatter)
   
   parser.add_argument('start_year', type=int, help='starting year, eg. 1990', default=None)
   parser.add_argument('end_year', type=int, help='ending year, eg. 2021', default=None)
   parser.add_argument('--mode', choices=['db','csv', 'query'], help='export mode', default='csv')
   parser.add_argument('--site', nargs="*", help='site code', default=None)
   parser.add_argument('--matrix', nargs="*", help='matrix', default=None)
   parser.add_argument('--components', nargs="*", help='component names', default=None)
   parser.add_argument('--output', type=str, help='output path', default=None)
      
   args = parser.parse_args()
   
   downloader = EbasDownloader(args.output)
   
   if args.mode=="csv":
      # export to csv file
      downloader.get_raw_files(conditions=args)
      csv_exporter = csvExporter(args.output)
      csv_exporter.export_csv()
   
   elif args.mode=="db":
      # create local database
      downloader.get_raw_files(conditions=args, download=True)
      db = EbasDB(args.output, dump='xz', detailed=True)
      db.update_db()
      
   elif args.mode=="query":
      db = EbasDB(args.output, dump='xz', detailed=True)
      db.init_db()
      print("Query Mode (ctrl+c to exit)")
      while True:
         condition ={}
         print("Enter selecting conditions, seperated by space, empty means select all:")
         id = input("site id:")
         if id!="":
            condition["id"] = id.strip().split(' ')
         
         matrix = input("matrix:")
         if matrix!="":
            condition["matrix"] = matrix.strip().split(' ')

         component = input("component:")
         if component!="":
            condition["component"] = component.strip().split(' ')

         st = input("starting date:")
         if st!="":
            condition["st"] = np.datetime64(st.strip())      

         ed = input("ending date:")
         if ed!="":
            condition["ed"] = np.datetime64(st.strip())  
         
         print(condition)
         df = db.query(condition, use_number_indexing=False)
         if df is not None:
            print(df.head(20))
         else:
            print("No data found!")
         
         export = input("Export selected dataframe? (y/n)")
         
         if export in ["y","yes","Y","Yes","YES"]:
            print("Exporting...")
            t= time.strftime("%Y%m%d%H%M")
            out_file = os.path.join(args.output, f"pyebas_{t}.csv")
            df.to_csv(out_file, index=False)
            print(f"Data has been exported to {out_file}.")



if __name__ =="__main__":   
   main()
      