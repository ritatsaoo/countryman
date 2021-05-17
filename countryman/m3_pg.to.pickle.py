import re
import pandas as pd # version at least 1.1.0
import geopandas as gpd

from sqlalchemy import create_engine, event
import Lily.ctao2.ctao2_database_alias
from Lily.ctao2.ctao2_database_alias import manidb, alias, tickwatch

if __name__ == '__console__' or __name__ == '__main__':

    workdir   = 'G:/NCREE_GIS/htax/' 
    
    #df_66000  = pd.read_pickle(workdir + 'data_pickle/bin_66000')
    #df_67000  = pd.read_pickle(workdir + 'data_pickle/bin_67000')
    #df_68000  = pd.read_pickle(workdir + 'data_pickle/bin_68000')
    df_91000  = pd.read_pickle(workdir + 'data_pickle/bin_91000')    
    df_92000  = pd.read_pickle(workdir + 'data_pickle/bin_92000')    
    
    
    df_10002  = pd.read_pickle(workdir + 'data_pickle/bin_10002')    
    #df_10008  = pd.read_pickle(workdir + 'data_pickle/bin_10008')
    #df_10009  = pd.read_pickle(workdir + 'data_pickle/bin_10009')
    #df_10010  = pd.read_pickle(workdir + 'data_pickle/bin_10010')
    #df_10013  = pd.read_pickle(workdir + 'data_pickle/bin_10013')
    #df_10014  = pd.read_pickle(workdir + 'data_pickle/bin_10014')
    #df_10015  = pd.read_pickle(workdir + 'data_pickle/bin_10015')
    #df_10016  = pd.read_pickle(workdir + 'data_pickle/bin_10016')
    #df_10017  = pd.read_pickle(workdir + 'data_pickle/bin_10017')
    #df_10018  = pd.read_pickle(workdir + 'data_pickle/bin_10018')
    #df_10020  = pd.read_pickle(workdir + 'data_pickle/bin_10020')

    #df_66000['LOCAT_ADDR'] = df_66000['LOCAT_ADDR'].apply( lambda x : x.split()[0] )
    #df_67000['LOCAT_ADDR'] = df_67000['LOCAT_ADDR'].apply( lambda x : x.split()[0] )
    #df_68000['LOCAT_ADDR'] = df_68000['LOCAT_ADDR'].apply( lambda x : x.split()[0] )
    df_91000['LOCAT_ADDR'] = df_91000['LOCAT_ADDR'].apply( lambda x : x.split()[0] )
    df_92000['LOCAT_ADDR'] = df_92000['LOCAT_ADDR'].apply( lambda x : x.split()[0] )
    
    df_10002['LOCAT_ADDR'] = df_10002['LOCAT_ADDR'].apply( lambda x : x.split()[0] )
    #df_10008['LOCAT_ADDR'] = df_10008['LOCAT_ADDR'].apply( lambda x : x.split()[0] )
    #df_10009['LOCAT_ADDR'] = df_10009['LOCAT_ADDR'].apply( lambda x : x.split()[0] )
    #df_10010['LOCAT_ADDR'] = df_10010['LOCAT_ADDR'].apply( lambda x : x.split()[0] )
    #df_10013['LOCAT_ADDR'] = df_10013['LOCAT_ADDR'].apply( lambda x : x.split()[0] )
    #df_10014['LOCAT_ADDR'] = df_10014['LOCAT_ADDR'].apply( lambda x : x.split()[0] )
    #df_10015['LOCAT_ADDR'] = df_10015['LOCAT_ADDR'].apply( lambda x : x.split()[0] )
    #df_10016['LOCAT_ADDR'] = df_10016['LOCAT_ADDR'].apply( lambda x : x.split()[0] )
    #df_10017['LOCAT_ADDR'] = df_10017['LOCAT_ADDR'].apply( lambda x : x.split()[0] )
    #df_10018['LOCAT_ADDR'] = df_10018['LOCAT_ADDR'].apply( lambda x : x.split()[0] )
    #df_10020['LOCAT_ADDR'] = df_10020['LOCAT_ADDR'].apply( lambda x : x.split()[0] )


    with manidb( workdir + 'nsg_bldg_3826.sqlite' ) as db:

        #db.get_alias('rawdata_hou_A66000_addr').write(df_66000)
        #db.get_alias('rawdata_hou_A67000_addr').write(df_67000)
        #db.get_alias('rawdata_hou_A68000_addr').write(df_68000)
        db.get_alias('rawdata_hou_A91000_addr').write(df_91000)
        db.get_alias('rawdata_hou_A92000_addr').write(df_92000)
          
        db.get_alias('rawdata_hou_A10002_addr').write(df_10002)
        #db.get_alias('rawdata_hou_A10008_addr').write(df_10008)
        #db.get_alias('rawdata_hou_A10009_addr').write(df_10009)
        #db.get_alias('rawdata_hou_A10010_addr').write(df_10010)
        #db.get_alias('rawdata_hou_A10013_addr').write(df_10013)
        #db.get_alias('rawdata_hou_A10014_addr').write(df_10014)
        #db.get_alias('rawdata_hou_A10015_addr').write(df_10015)
        #db.get_alias('rawdata_hou_A10016_addr').write(df_10016)
        #db.get_alias('rawdata_hou_A10017_addr').write(df_10017)
        #db.get_alias('rawdata_hou_A10018_addr').write(df_10018)
        #db.get_alias('rawdata_hou_A10020_addr').write(df_10020)
    print('1')

    df_64000  = pd.read_pickle(workdir + 'data_pickle/bin_64000')
    
    try: 
        df_64000['LOCAT_ADDR'] = df_64000['LOCAT_ADDR'].apply( lambda x : x.split()[0] )
    except:
        print("list index out of range")
    with manidb( workdir + 'nsg_bldg_3826.sqlite' ) as db:
    
        db.get_alias('rawdata_hou_A64000_addr').write(df_64000)
    