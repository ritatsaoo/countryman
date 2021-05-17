#the "Answer to the Ultimate Question of Life, the Universe, and Everything"
import json
from datetime import datetime
from Lily.ctao2.ctao2_nsgstring import alnum_uuid
from Lily.ctao2.ctao2_hostmetadata import hostmetadata
import Lily.ctao2.ctao2_database_alias
from Lily.ctao2.ctao2_database_alias import manidb, alias, tickwatch

def split_list_to_chunks(lst: list, number: int) -> list:
    '''Yield successive n-sized chunks from lst.'''
    return [ lst[i:i + number] for i in range(0, len(lst), number) ]

def numbering_list(lst: list) -> list:
    '''替每一筆資料 編上流水號'''
    return [ [num, row] for num, row in zip( range(len(lst)) , lst) ]

def google_map_api(addr:str) ->dict:
    import requests
    #google_api_key
    api_key     = f'''AIzaSyC9OvuNmrvtmNvVORtWEjo5GyLyr21yATc'''
    url_address = f'''https://maps.googleapis.com/maps/api/geocode/json?address={addr}&key={api_key}'''
    
    #讀取網址的json檔
    response                        = requests.get(url_address)
    #print(response.apparent_encoding)確認是否為utf-8
    response.encoding               ='utf-8'
    jsondata                        = response.text
    #抓取特定資料(抓取更深入且特定的資料並顯示出)
    jsondata                        = json.loads(jsondata)

    rdset  = {}  
    if jsondata['status'] == 'OK' :            
        rdset['lat']                = jsondata['results'][0]['geometry']['location']['lat']
        rdset['lng']                = jsondata['results'][0]['geometry']['location']['lng']
        rdset['formatted_address']  = jsondata['results'][0]['formatted_address']
        rdset['location_type']      = jsondata['results'][0]['geometry']['location_type']
        rdset['json']               = jsondata
    return rdset

class answer:
    def __init__(self):
        self.tw_county_code_list    = ['10002', '10004', '10005', '10007', '10008', '10009', '10010', 
                                       '10013', '10014', '10015', '10016', '10017', '10018', '10020', 
                                       '63000', '64000', '65000', '66000', '67000', '68000', '91000', '92000']

        self.ktt_county_code_list   = ['63000', '65000', '10002']

        self.ol_county_code_list    = ['10016', '91000', '92000']

        self.uuid                   = alnum_uuid()
        self.host                   = hostmetadata()
        self.begtime                = datetime.now()
        self.msgtime                = [['beg', self.begtime, 0]]            

    def tick(self):
        import inspect
        curframe                    = inspect.currentframe()
        calframe                    = inspect.getouterframes(curframe, 2)
        caller_name                 = calframe[1][3]

        time_point                 = datetime.now()
        time_diff                  = (time_point - self.begtime)
                   
        seconds                    = (time_diff).seconds 
        ms                         = (time_diff).microseconds 

        self.msgtime.append( [ caller_name, time_point, seconds  ] )
        return '{2}_({0:03}s).({1:06}ms)\t\t'.format(seconds, ms, self.uuid)


if __name__ == '__console__' or __name__ == '__main__':
    import pandas
    from simpledbf import Dbf5
    from multiprocessing import Pool
    mpool        = Pool(8)
    srcdb_file  =  r'G:/NCREE_GIS/Solution_Book/countryman/countryman/_Total_err3_s2.dbf'

    dbf         = Dbf5 (srcdb_file, codec='big5')
    df          = dbf.to_dataframe()#.head(20)
    addr        = df['CNTY_NAME'].fillna(value='') + df['TOWN_NAME'].fillna(value='') + df['ATRACTNAME'].fillna(value='') + df["AROAD"].fillna(value='') + df["AAREA"].fillna(value='') + df["ALANE"].fillna(value='') + df["AALLEY"].fillna(value='') + df["ANO"].fillna(value='')
    api         = mpool.map(google_map_api, addr)
    
    list_lat = []
    list_lng = []
    list_location_type = []

    for i in api:
        if i != {}:
            print(i)
            list_lat.append(i['lat'])
            list_lng.append(i['lng'])
            list_location_type.append(i['location_type'])
        else:
            list_lat.append('')
            list_lng.append('')
            list_location_type.append('')
    
    df['GOOGLE_LAT']    = list_lat
    df['GOOGLE_LON']    = list_lng
    df['location_type'] = list_location_type

    output      = manidb('G:/NCREE_GIS/2020_address/_Total_err3_s2.sqlite')
    
    output.get_alias('_Total_err3_s2_V1').write(df)