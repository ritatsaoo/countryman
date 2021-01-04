import csv, re 
import numpy as np
import pandas as pd
import math
from itertools import zip_longest

from Lily.ctao2.ctao2_database_alias import manidb, alias, tickwatch
from multiprocessing import Pool

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


#%% read csv
def fun4cntycode(arg):
    res1     = {}
    arg0     = arg[0]
    arg1     = arg[1]
    if arg1 == arg0:
        res1['reCntycode'] = 1
    else:
        res1['reCntycode'] = 0
    return res1

def fun4towncode(arg):
    res1     = {}
    arg0     = arg[0]
    arg1     = arg[1]
    if arg1[:5] == arg0:       
        res1['reTowncode'] = 1
    else:
        res1['reTowncode'] = 0
    return res1

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
    
    # ^([地下]{0,2}([0-9]{0,4}[樓層]{1})){0,7}([\-]{1}[0-9A-ZＡ-Ｚ]{0,3})$
    FLOORd = r'^([地下底室\-]{0,4}([0-9A-Z,]{0,4}[樓層]{1}[0-9A-Z,\-]{0,7}){0,4}){0,10}([\-]{0,1}[0-9A-Z]{0,3}){0,7}$'

    repattern       = re.compile(NUMd)
    match           = re.match(repattern, res1['NUMBER'] ) 
    
    if match:
        res1['reNUM'] = 1 
    else:
        res1['reNUM'] = 0

    repattern      = re.compile(FLOORd)
    match          = re.match(repattern, res1['FLOOR'] )
  
    if match:
        res1['reFLOOR'] = 1
    else:
        res1['reFLOOR'] = 0 

    return res1

#df0 , source df
def check_addr_column(cnty_source_name, cnty_source_df, col_name , fun_obj):
    mpool          = Pool(8)   

    list_arg       = cnty_source_df[col_name].tolist()
    a              = [cnty_source_name]*len(list_arg)
    ziparg         = zip(a, list_arg)

    result_trans   = mpool.map(fun_obj, ziparg)
    
    cputime.tick()

    df = pd.DataFrame.from_dict(result_trans, orient = 'columns')

    for colname in df.columns:
        cnty_source_df[colname] = df[colname]

    mpool.close()
    return cnty_source_df

if __name__ == '__console__' or __name__ == '__main__':
    cputime = tickwatch()

    mydb = manidb('G:/NCREE_GIS/2020_address/TGOS_NLSC_TWN22.sqlite')
    cnty = mydb.get_alias('metadata_nsg_cnty').read()
    #for key, row in cnty.iterrows():
        #cntynum = row['ncity']
        #target = 'A' + cntynum
 
    for cntynum in ['66000']:
        target = 'A' + cntynum
        
        #resource
        tab0 = mydb.get_alias(target)
        #outcome
        tab1 = mydb.get_alias(target + '_TRAN' )
        tab2 = mydb.get_alias(target + '_TRAN_out' )

        df0  = tab0.read()
    
        if df0.empty :
            continue

        cputime.tick()
        
        df0 = check_addr_column(cntynum, df0, 'cnty_code', fun4cntycode)
        #df0 = check_addr_column(cntynum, df0, 'town_code', fun4towncode)
        #df0 = check_addr_column(cntynum, df0, 'num', fun4number)
        cputime.tick('calculation accomplished')
        tab1.write(df0)

        # export the outlier
        df1 = df0[df0['reCntycode']==0]
        df2 = df0[df0['reTowncode']==0]
        df3 = df0[df0['reNUM']     ==0]
        frames = [df1, df2, df3]

        result = pd.concat(frames)
        tab2.write(result)

        cputime.tick('write down dataframe' + target)
