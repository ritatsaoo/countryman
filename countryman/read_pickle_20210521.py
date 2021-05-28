import re
import pandas as pd # version at least 1.1.0
import geopandas as gpd

from sqlalchemy import create_engine, event
import Lily.ctao2.ctao2_database_alias
from Lily.ctao2.ctao2_database_alias import manidb, alias, tickwatch

if __name__ == '__console__' or __name__ == '__main__':

    workdir          = 'G:/NCREE_GIS/htax/' 
    country          = '92000'
    df               = pd.read_pickle(workdir + f'data_pickle/ext_{country}')
    #df['LOCAT_ADDR'] = df['LOCAT_ADDR'].apply( lambda x : x.split()[0] )
    
    
    with manidb( workdir + 'nsg_bldg_hinfo.sqlite' ) as db:
        db.get_alias(f'rawdata_hou_A{country}_hinfo').write(df)
    