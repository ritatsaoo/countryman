import re
import pandas as pd # version at least 1.1.0
import geopandas as gpd

from sqlalchemy import create_engine, event
import Lily.ctao2.ctao2_database_alias
from Lily.ctao2.ctao2_database_alias import manidb, alias, tickwatch

class node:
    def __init__(self, value):
        self.val = value
        self.left = None
        self.right = None
    def setLeft(self, left):
        self.left = left
    def setRight(self, right):
        self.right = right  


p1 = node('town')
root = p1
p2 = node('tract')
p3 = node('lin')
p4 = node('road')
p5 = node('zone')
p6 = node('number')
p1.setLeft(p2)
p1.setRight(p3)
p1.setRight(p4)
p1.setRight(p5)
p1.setRight(p6)

p2.setLeft(p3)
p2.setLeft(p3)
p2.setLeft(p3)
p2.setLeft(p3)
p2.setLeft(p3)
p2.setRight(p5)
p3.setRight(p7)

if __name__ == '__console__' or __name__ == '__main__':

    workdir          = 'G:/NCREE_GIS/htax/' 
    country          = '92000'
    df               = pd.read_pickle(workdir + f'data_pickle/ext_{country}')
    #df['LOCAT_ADDR'] = df['LOCAT_ADDR'].apply( lambda x : x.split()[0] )
    
    
    with manidb( workdir + 'nsg_bldg_hinfo.sqlite' ) as db:
        db.get_alias(f'rawdata_hou_A{country}_hinfo').write(df)
    