import re
import pandas
import csv, re, math, json
import numpy as np
import pandas as pd
from itertools import zip_longest
import Lily.ctao2.ctao2_database_alias
from Lily.ctao2.ctao2_database_alias import manidb, alias, tickwatch
from multiprocessing import Pool
import osgeo, ogr



if __name__ == '__console__' or __name__ == '__main__':
    cputime         = tickwatch()
    #%%----------read-------------
    origindb        = manidb( 'G:/NCREE_GIS/htax/nsg_bldg_taipei.sqlite' ) 
    country         = 'taipei'
    df_tax          = origindb.get_alias(f'htax_{country}').read()
    df_tgo          = origindb.get_alias(f'tgos_{country}_group').read()[['nsg_addr_key','geom']]#.head(200)
    cputime.tick('Dataframe read')
 
    #%%---------search-------------

    df_tax = df_tax.merge(df_tgo, left_on='nsg_addr_key', right_on='nsg_addr_key')

    #df_tgo          = df_tgo.set_index(['nsg_addr_key'])
    #tax_addr_list   = df_tax['nsg_addr_key'].tolist()
    #i = 0
    #for addr in tax_addr_list:
    #    i += 1; print(i)
    #    if addr in df_tgo.index:
    #        df_tax['geom'] = df_tgo.loc[addr, 'geom']
    #        print(addr)
    #    else:
    #        df_tax['geom'] = ''
    #        print('no')


    cputime.tick('Searching done')
 
    #%%---------writing-------------


    select_cols                  = ["HOU_LOSN", "geom","nsg_addr_key", "addr", "town", "tract", "lin", "road", "zone", "lane", "alley", "num_p", "num_1", "num_2", "floor_p", "floor_1", "floor_2", "memo", "dc_json_report"]
    df_tax                       = df_tax[ select_cols ]

    origindb.get_alias(f'combination_{country}').write(df_tax)
    
    cputime.tick('Written done')

