import csv, re 
import numpy as np
import pandas as pd
import math
from itertools import zip_longest
import Lily.ctao2.ctao2_database_alias
from Lily.ctao2.ctao2_database_alias import manidb, alias, tickwatch
from multiprocessing import Pool

#from osgeo import ogr
import osgeo, ogr
#%% text conversion
CT2_HTAB_SECT_NUM = str.maketrans (
    {
    '0':'○',
    'ㄧ':'一',
    '1':'一', 
    '2':'二',
    '3':'三',
    '4':'四',
    '5':'五',
    '6':'六',
    '7':'七',
    '8':'八',
    '9':'九',
#０-９ 65296-65305
    65296:'○',
    65297:'一', 
    65298:'二', 
    65299:'三', 
    65300:'四', 
    65301:'五', 
    65302:'六', 
    65303:'七', 
    65304:'八',
    65305:'九'
    })

CT2_HTAB_CHN = str.maketrans (
    {
    '巿':'市',
    '臺':'台',
    '褔':'福', 
    '豊':'豐',
    '陜':'陝',
    '恒':'恆',
    'ㄧ':'一'
    })

CT2_HTAB_ENG = str.maketrans(
    {# uppercase
    'Ａ':'A',
    'Ｂ':'B',
    'Ｃ':'C',
    'Ｄ':'D',
    'Ｅ':'E',
    'Ｆ':'F',
    'Ｇ':'G',
    'Ｈ':'H',
    'Ｉ':'I',
    'Ｊ':'J',
    'Ｋ':'K',
    'Ｌ':'L',
    'Ｍ':'M',
    'Ｎ':'N',
    'Ｏ':'O',
    'Ｐ':'P',
    'Ｑ':'Q',
    'Ｒ':'R',
    'Ｓ':'S',
    'Ｔ':'T',
    'Ｕ':'U',
    'Ｖ':'V',
    'Ｗ':'W',
    'Ｘ':'X',
    'Ｙ':'Y',
    'Ｚ':'Z',
    # lowercase
    'ａ':'a',
    'ｂ':'b',
    'ｃ':'c',
    'ｄ':'d',
    'ｅ':'e',
    'ｆ':'f',
    'ｇ':'g',
    'ｈ':'h',
    'ｉ':'i',
    'ｊ':'j',
    'ｋ':'k',
    'ｌ':'l',
    'ｍ':'m',
    'ｎ':'n',
    'ｏ':'o',
    'ｐ':'p',
    'ｑ':'q',
    'ｒ':'r',
    'ｓ':'s',
    'ｔ':'t',
    'ｕ':'u',
    'ｖ':'v',
    'ｗ':'w',
    'ｘ':'x',
    'ｙ':'y',
    'ｚ':'z',
    })

CT2_HTAB_NUM = str.maketrans(
    {
    '○':'0',
    'ㄧ':'1',
    '一':'1', 
    '二':'2',
    '三':'3',
    '四':'4',
    '五':'5',
    '六':'6',
    '七':'7',
    '八':'8',
    '九':'9',
    '十':'10',
#０-９ 65296-65305
    65296:'0', 
    65297:'1', 
    65298:'2', 
    65299:'3', 
    65300:'4', 
    65301:'5', 
    65302:'6', 
    65303:'7', 
    65304:'8',
    65305:'9'
    })

CT2_HTAB_SYMBOL = str.maketrans(
   {# brackets
    '（' :'(',
    '﹙' :'(',
    '『' :'(',
    '「' :'(',
    '﹝' :'(',
    '〔' :'(',
    '｛' :'(',
    '【' :'(',
    '《' :'(',
    '〈' :'(',

    '）' :')',
    '﹚' :')',
    '』' :')',
    '」' :')',
    '﹞' :')',
    '〕' :')',
    '｝' :')',
    '】' :')',
    '》' :')',
    '〉' :')',
    # comma
    '、' :',',
    '，' :',',
    # desh, hyphen
    '﹣' :'-',
    '–'  :'-',
    '－' :'-',
    '─'  :'-',
    '—'  :'-',
    '＿' :'-',
    'ˍ'  :'-',
    '▁'  :'-',
    '之' :'-',
    '附' :'-',
    # tilde
    '～' :'~',
    # colon
    '：':':',
    '﹕':':',
    # semicolon
    '；':';',
    '﹔':';',
    # question
    '？':'?',
    '﹖':'?',
    # exclamation
    '！':'!',
    '﹗':'!',
    # slash
    '╱' :"/",
    '／':"/",
    '∕' :"/",
    # hashtag
    '＃':'#',
    "#":'#',
    # quotation
    '‘' :'"',
    '’' :'"',
    '“' :'"',
    '”' :'"',
    '〝':'"',
    '〞':'"',
    '‵' :'"'
    })

def fun4checkgeo(arg):
    res1     = {'checkgeo' : 0}
    try:
        bnd     = ogr.CreateGeometryFromWkt(arg[0])
        poi     = ogr.CreateGeometryFromWkt(arg[1])
        res1['checkgeo'] = poi.Within(bnd)  
    except:
        res1['checkgeo'] = 3
    return res1

def fun4cntycode(arg):
    return {'reCntycode' : (arg[1] == arg[0]) }

def fun4towncode(arg):
    return {'reTowncode' : (arg[1][:5] == arg[0]) }

def fun4number(arg):
    res1     = {}
    arg = arg[1]
    if arg is None:
        res1['NUMBER'] = ''
        res1['FLOOR']  = ''
        return res1

    ans1 = arg.split('號')
    
    if len(ans1) == 2:
        res1['NUMBER'] = ans1[0].translate(CT2_HTAB_NUM).translate(CT2_HTAB_SYMBOL)
        res1['FLOOR']  = ans1[1].translate(CT2_HTAB_NUM).translate(CT2_HTAB_SYMBOL)
    else:
        res1['NUMBER'] = ans1[0].translate(CT2_HTAB_NUM).translate(CT2_HTAB_SYMBOL)
        res1['FLOOR']  = ''

    #step 1 NUMd  = r'[0123456789]{1,4}'
    #step 2 NUMd  = r'[臨]{0,1}[0123456789]{1,4}'
    #step 3 NUMd  = r'^[臨]{0,1}[0123456789]{1,4}$'
    #step 4 NUMd  = r'^[臨]{0,1}[0-9\-]{1,4}$'
    #step 5 NUMd  = r'^[臨]{0,1}[0-9]{1,4}[\-0-9]{0,5}$'
 
    NUMd  = r'^([臨東建附特()]{0,3})([0-9]{1,4})([衖]{1}[0-9A-Z]{0,6}){0,3}([\-]{1}[0-9A-Z甲乙丙丁]{0,6}){0,3}$'

    FLOORd = r'^([地下底室\-]{0,4}([0-9A-Z,]{0,4}[樓層]{1}[0-9A-Z,\-]{0,7}){0,4}){0,10}([\-]{0,1}[0-9A-Z]{0,3}){0,7}$'

    repattern       = re.compile(NUMd)
    match           = re.match(repattern, res1['NUMBER'] ) 
    
    if match:
        res1['reNumber'] = 1
    else:
        res1['reNumber'] = 0

    repattern      = re.compile(FLOORd)
    match          = re.match(repattern, res1['FLOOR'] )
  
    if match:
        res1['reFloor'] = 1
    else:
        res1['reFloor'] = 0 

    return res1

def fun4alley(arg):
    res1     = {}
    if arg is None:
        res1['alley2'] = ''
        res1['number'] = ''
        return res1
    if (u'衖') in arg:
        ans0 = arg.split(u'衖')
        res1['alley2'] = ans0[0]+ u'衖'

        res1['number'] = ans0[1] 
    else:
        res1['alley2'] = ''
        res1['number'] = arg  
    return res1

def lookup_value(arg1, arg2, arg3):
    list0 = []; dic = {}
    list_town_code = arg1[arg2].tolist()
    list_point_geo = arg1['point_wkt'].tolist()

    df = arg3.set_index(arg2)
    for town_code in list_town_code:
        town_wkt = 0
        if town_code in df.index:
            town_wkt = df.loc[town_code, 'town_wkt']
        else:
            town_wkt = ''
        list0.append(town_wkt)

    dic['town_wkt'] = list0
    df = pd.DataFrame.from_dict(dic, orient = 'columns')
    return df

def check_addr_column(cnty_source_name, cnty_source_df, col_name , fun_obj):
    mpool          = Pool(8)   

    list_arg       = cnty_source_df[col_name].tolist()
    a              = [cnty_source_name]*len(list_arg)
    ziparg         = zip(a, list_arg)

    result_trans   = mpool.map(fun_obj, ziparg)


    df = pd.DataFrame.from_dict(result_trans, orient = 'columns')

    for colname in df.columns:
        cnty_source_df[colname] = df[colname]

    mpool.close()
    return cnty_source_df

def output_error(mydb, df0, target):

    cputime = tickwatch()

    df1    = df0[df0['check_town_geo']==0]
    df2    = df0[df0['reCntycode']    ==0]
    df3    = df0[df0['reTowncode']    ==0]
    df4    = df0[df0['reNumber']      ==0]
    frames = [df1, df2, df3, df4]

    result = pd.concat(frames)
    result = result[['origin_address','fid', 'cnty_code', 'town_code', 'lie', 'lin', 'road', 'zone', 'lane', 'alley', 'number','floor','checklist']]
    name   = target + '_modify'

    mydb.get_alias(name).write(result)
    cputime.tick('Write down dataframe' + name)

if __name__ == '__console__' or __name__ == '__main__':
    
    cputime     = tickwatch()

    mydb        = manidb('G:/NCREE_GIS/2020_address/20210127_TGOS_NLSC_TWN22.sqlite')
    target      = 'A68000_TRAN0'
    df0         = mydb.get_alias(target).read()
    cputime.tick('Data read')

    alleylist   = df0['alley'].to_list()
    numberlist  = df0['number'].to_list()

    mpool       = Pool(8)
    dfalley     = mpool.map(fun4alley, numberlist)
    mpool.close()

    #for arg in numberlist:
    #    fun4alley(arg)
    #dfalley={}
    #for arg in numberlist:
        #dfalley.append(fun4alley(numberlist))

    df_alley   = pd.DataFrame.from_dict(dfalley, orient = 'columns')
    cputime.tick('Split done')

    sdf            = df0[['origin_address', 'fid', 'cnty_code', 'town_code', 'lie', 'lin', 'road', 'zone', 'lane', 'alley']]
    sdf['alley2']  = df_alley['alley2']
    sdf['number']  = df_alley['number']
    sdf['floor']   =  df0['floor']
    sdf['checklist'] =  df0['checklist']
    
    mydb.get_alias(target + '_TRAN' ).write(sdf)
    cputime.tick('Write down dataframe' + target)