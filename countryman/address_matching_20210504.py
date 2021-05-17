import csv, re 
import numpy as np
import pandas as pd
import math, json
from itertools import zip_longest
import Lily.ctao2.ctao2_database_alias
from Lily.ctao2.ctao2_database_alias import manidb, alias, tickwatch
from multiprocessing import Pool
import osgeo, ogr

class countryman4uniform:
    def __init__(self):
        ## number tranform ##
        self.uni_num2txt_list =  str.maketrans ({
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
                                65305:'九',
                                })
        self.uni_full2half_list =  str.maketrans ({
                                'Ｏ':'0',
                                '○':'0',
                                'ㄧ':'1',
                                '一':'1', 
                                '二':'2',
                                '兩':'2',
                                '三':'3',
                                '四':'4',
                                '五':'5',
                                '六':'6',
                                '七':'7',
                                '八':'8',
                                '九':'9',
                                '十':'10',

                                '零':'0',
                                '壹':'1', 
                                '貳':'2',
                                '參':'3',
                                '肆':'4',
                                '伍':'5',
                                '陸':'6',
                                '柒':'7',
                                '捌':'8',
                                '玖':'9',
                                '拾':'10',
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
                                65305:'9',
                                '之' :'-'
                                })
        ## mandarin tranform ##
        self.uni_text_list =   str.maketrans ({
                                '双':'雙',
                                '邨':'屯',
                                '刣':'台',
                                '坟':'汶',
                                '坂':'板',
                                '芉':'竿',
                                '壳':'殼',
                                '尫':'尪',
                                '坔':'湳',
                                '𡶛':'卡',
                                '拕':'托',
                                '岺':'苓',
                                '庙':'廟',
                                '畑':'煙',
                                '胆':'膽',
                                '响':'響',
                                '恒':'恆',
                                '𫔘':'閂',
                                '畓':'沓',
                                '羗':'羌',
                                '峯':'峰',
                                '笋':'筍',
                                '梹':'檳',
                                '硘':'回',
                                '梘':'見',
                                '𦰡':'那',
                                '躭':'耽',
                                '脚':'腳	',
                                '猪':'豬',
                                '𥿄':'紙',
                                '啓':'啟',
                                '亀':'龜',
                                '戞':'戛',
                                '鈎':'勾',
                                '硦':'弄',
                                '焿':'庚',
                                '菓':'果',
                                '湶':'泉',
                                '堺':'界',
                                '犂':'犁',
                                '厦':'廈',
                                '萡':'箔',
                                '厨':'廚',
                                '猫':'貓',
                                '𡍼':'塗',
                                '畬':'舍',
                                '𦋐':'罩',
                                '嵵':'時',
                                '葱':'蔥',
                                '竪':'豎',
                                '塩':'鹽',
                                '獇':'猐',
                                '䧟':'陷',
                                '槺':'康',
                                '廍':'部',
                                '皷':'鼓',
                                '𢊬':'廩',
                                '𨻶':'隙',
                                '𩵺':'月',
                                '胆':'曼',
                                '蔴':'麻',
                                '噍':'焦',
                                '窰':'窯',
                                '磘':'嗂',
                                '関':'關',
                                '鮘':'代',
                                '磜':'祭',
                                '𥕟':'漏',
                                '𥕢':'槽',
                                '舘':'館',
                                '𡒸':'層',
                                '藔':'寮',
                                '壠':'壟',
                                '𫙮':'桀',
                                '鐤':'鼎',
                                '𧃽':'應',
                                '鷄':'雞',
                                '欍':'舊',
                                '𩻸':'逮',
                                '鑛':'礦',
                                '灧':'艷',
                                '𣐤':'舊',
                                '効':'效',
                                '温':'溫',
                                '敍':'敘',
                                '𥔽':'塔',
                                '卧':'臥',
                                '凉':'涼',
                                '巿':'市',
                                '臺':'台',
                                '褔':'福', 
                                '豊':'豐',
                                '陜':'陝',
                                '恒':'恆',
                                'ㄧ':'一'
                                })
        ## alphabet tranform ##
        self.uni_alphabet_list =  str.maketrans({
                                # uppercase
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
        ## symbol tranform ##
        self.uni_symbol_list =  str.maketrans({
                                # brackets
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
                                '‧'  : ',', 
                                '丶' : ',',
                                '、' : ',',
                                '，' : ',',
                                '。' : ',',
                                '＆' : ',',
                                '．' : ',',
                                # desh, hyphen
                                '﹣' :'-',
                                '–'  :'-',
                                '－' :'-',
                                '─'  :'-',
                                '—'  :'-',
                                '＿' :'-',
                                'ˍ'  :'-',
                                '▁'  :'-',
                                '―'  :'-',
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
    
    def text_uniform(self, addr_text):
        return addr_text.translate(self.uni_text_list)
    
    def number2txt_uniform(self, addr_text):
        return addr_text.translate(self.uni_num2txt_list)
    
    def full2half_uniform(self, addr_text):
        return addr_text.translate(self.uni_full2half_list)
    
    def alphabet_uniform(self, addr_text):
        return addr_text.translate(self.uni_alphabet_list)

    def symbol_uniform(self, addr_text):
        return addr_text.translate(self.uni_symbol_list)
    
    def txt2number_uniform(self, addr_text):
        num_chn= r'^[一二三四五六七八九十零壹貳參肆伍陸柒捌玖拾兩千仟百佰]{1,5}$'
        num_xx0= r'(^[十拾]$)'
        num_xx1= r'(^[一二三四五六七八九壹貳參肆伍陸柒捌玖]$)'
        num_xx2= r'(^[十拾])([一二三四五六七八九壹貳參肆伍陸柒捌玖]$)'
        num_xx3= r'(^[一二三四五六七八九壹貳參肆伍陸柒捌玖])([十拾]$)'
        num_xx4= r'(^[一二三四五六七八九壹貳參肆伍陸柒捌玖])([十拾])([一二三四五六七八九壹貳參肆伍陸柒捌玖]$)'
        num_xh1= r'(^[一二三四五六七八九壹貳參肆伍陸柒捌玖])([百佰])$'
        num_xh2= r'(^[一二三四五六七八九壹貳參肆伍陸柒捌玖])([百佰])(零)([一二三四五六七八九壹貳參肆伍陸柒捌玖])$'
        num_xh3= r'(^[一二三四五六七八九壹貳參肆伍陸柒捌玖])([百佰])([一二三四五六七八九壹貳參肆伍陸柒捌玖])([十拾])$'
        num_xh4= r'(^[一二三四五六七八九壹貳參肆伍陸柒捌玖])([百佰])([一二三四五六七八九壹貳參肆伍陸柒捌玖])([十拾])([一二三四五六七八九壹貳參肆伍陸柒捌玖])$'
        
        
        if not re.match(num_chn, addr_text):
            return addr_text

        if re.match(num_xx0, addr_text):
            addr_text = '10'

        elif re.match(num_xx2, addr_text):
            addr_text = re.sub(num_xx2, '1\g<2>', addr_text)

        elif re.match(num_xx3, addr_text):
            addr_text = re.sub(num_xx3, '\g<1>0', addr_text)

        elif re.match(num_xx4, addr_text):
            addr_text = re.sub(num_xx4, '\g<1>\g<3>', addr_text)

        elif re.match(num_xh1, addr_text):
            addr_text = re.sub(num_xh1, '\g<1>00', addr_text)

        elif re.match(num_xh2, addr_text):
            addr_text = re.sub(num_xh2, '\g<1>0\g<4>', addr_text)

        elif re.match(num_xh3, addr_text):
            addr_text = re.sub(num_xh3, '\g<1>\g<3>0', addr_text)

        elif re.match(num_xh4, addr_text):
            addr_text = re.sub(num_xh4, '\g<1>\g<3>\g<5>', addr_text)

        return addr_text.translate(self.uni_full2half_list)

class countryman4wkt:
    def  __init__(self, dataframe1):

        self.mydf = dataframe1

    def pointwkt(self, df0):
        
        df0['point_wkt'] = self.mydf[['TWD97_X', 'TWD97_Y']].astype(str).apply(lambda x: ' '.join(x), axis=1)
        df0['point_wkt'] = df0['point_wkt'].apply (lambda x : f'POINT({x})')
        return df0

    def townwkt(self, town):
        list0 = []; dic = {}
        list_town_code = self.mydf['town_code'].tolist()

        df = town.set_index('town_code')
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

class countryman4check:
    
    def __init__(self, cntynum, df):
        self.cntynum   = cntynum
        self.df        = df

    def fun4geo(self, wkt):
        town_wkt       = wkt[0]
        point_wkt      = wkt[1]
        res1           = {'checkgeo' : 0}
        try:
            bnd        = ogr.CreateGeometryFromWkt(town_wkt)
            poi        = ogr.CreateGeometryFromWkt(point_wkt)
            res1['checkgeo'] = poi.Within(bnd)  
        except:
            res1['checkgeo'] = 3
        return res1

    def fun4cntycode(self, cntycode):
        #return {'reCntycode' : (cntycode == self.cntynum) }
        return {cntycode == self.cntynum}

    def fun4towncode(self, towncode):
        #return {'reTowncode' : (towncode[:5] == self.cntynum) }
        return {towncode[:5] == self.cntynum}

    def fun4number(self, num):
        res1  = {}
        NUMP        = r'([臨建附特])?'
        NUMd        = r'([０-９Ａ-Ｚ甲乙丙丁]{1,6})'
        NUM_        = r'([\-之]{1}[０-９Ａ-Ｚ0-9]{1,3})?'

        FLOORP      = r'([地上下底室層]{1,3})?'
        FLOORb      = r'([一-十四甲乙丙丁,０-９壹貳參肆伍陸柒捌玖]{0,6}[棟]{1})?'
        FLOORd      = r'([一-十四甲乙丙丁,０-９壹貳參肆伍陸柒捌玖]{0,6}[樓層]{1}[０-９Ａ-Ｚ]{0,2})?' 
        FLOOR_      = r'([\-之]{1}[0-9０-９Ａ-Ｚ一-十四]{1,3})?'
        room        = r'([一-十四甲乙丙丁,０-９壹貳參肆伍陸柒捌玖]{0,6}[室]{1})?'

        NUM_ALL     = f'''^{NUMP}{NUMd}{NUM_}{NUM_}號{NUM_}{NUM_}'''
        FLO_ALL     = f'''^{FLOORP}{FLOORb}{FLOORd}{FLOOR_}{FLOOR_}{room}'''
       
        match      = re.match(NUM_ALL, str(num))
        if match:

            prefix = match.group(0)

            #res1['num_head']  = prefix
            #res1['num_tail']  = num[len(prefix) : ]

            ##mglist            = [ str(x or '') for x in match.groups()]
            #res1['num_p']     = str(match.groups()[0] or '')
            #res1['num_1']     = str(match.groups()[1] or '') + str(match.groups()[2] or '') + str(match.groups()[3] or '')
            #res1['num_2']     = str(match.groups()[4] or '') + str(match.groups()[5] or '')


            #res1['num_SEGM']  = str(num)

            a = num[len(prefix) : ]
            match2     =re.match(FLO_ALL, a)    
        
            if match2:
                #res1['floor']            = match2.group(0)
                res1['floor_p']          = str(match2.groups()[0] or '') + str(match2.groups()[1] or '')
                res1['floor_1']          = str(match2.groups()[2] or '') 
                res1['floor_2']          = str(match2.groups()[3] or '') + str(match2.groups()[4] or '') + str(match2.groups()[5] or '')                
                for value in a[len(match2.group(0)) : ]:
                    if '同意' in value:
                        print(value,type(value))
                        res1['demand']   = a[len(match2.group(0)) : ]
                        res1['memo']     = ''
                    else:
                        res1['demand']   = ''
                        res1['memo']     = a[len(match2.group(0)) : ]
        return res1

    def fun4floor(self, floor):
        res = {}
        if '樓' in floor:
            head = floor.split('樓')[0]
            tail = floor.split('樓')[1:]
            for i in tail:
                res['floor_1'] = countryman4uniform().txt2number_uniform(head)#+'樓'+ countryman4uniform().full2half_uniform(i)
        else:
            res['floor_1'] = floor

        return res

def uni(arg, command):

    if command == 'full2half_uniform':
        a = mpool.map(countryman4uniform().full2half_uniform, arg)
    elif command == 'number2txt_uniform':
        a = mpool.map(countryman4uniform().number2txt_uniform, arg)
    elif command == 'text_uniform':
        a = mpool.map(countryman4uniform().text_uniform, arg)
    elif command == 'alphabet_uniform':
        a = mpool.map(countryman4uniform().alphabet_uniform, arg)
    elif command == 'symbol_uniform':
        a = mpool.map(countryman4uniform().symbol_uniform, arg)
    elif command == 'txt2number_uniform':
        a = mpool.map(countryman4uniform().txt2number_uniform, arg)
    
    return a

def check(cntynum, df0, arg, command):

    if command == 'cntycode_check':
        a = mpool.map(countryman4check(cntynum, df0).fun4cntycode, arg)
    elif command == 'towncode_check':
        a = mpool.map(countryman4check(cntynum, df0).fun4towncode, arg)
    elif command == 'geo_check':
        a = mpool.map(countryman4check(cntynum, df0).fun4geo, arg)
    elif command == 'num_check':
        a = mpool.map(countryman4check(cntynum, df0).fun4number, arg)
    elif command == 'floor_check':
        a = mpool.map(countryman4check(cntynum, df0).fun4floor, arg)
    
    return a

def get_df(df0, list):
    
    df = pd.DataFrame.from_dict(list, orient = 'columns')
    for colname in df.columns:
        df0[colname] = df[colname].fillna(value='')
    return df0


if __name__ == '__console__' or __name__ == '__main__':
    cputime     = tickwatch()
    #%%-----------resource--------------
    mydb        = manidb('G:/NCREE_GIS/tgos_address/2021_TGOS_NLSC_TWN22_V1.sqlite')
    output      = manidb('G:/NCREE_GIS/tgos_address/nsg_bldg_TGOS.sqlite')
    
    #cnty        = mydb.get_alias('metadata_nsg_cnty').read()
    cnty       = mydb.get_alias('metadata_nsg_cnty_3825').read()
    town        = mydb.get_alias('metadata_nsg_town').read()

    for key, row in cnty[ cnty['ncity'] =='09020'].iterrows():
    #for key, row in cnty.iterrows():
        
        #%%----------read-------------
        mpool        = Pool(8)
        
        cntynum      = row['ncity']
        cnty_wkt     = row['cnty_wkt']
        
        original     = 'A' + cntynum
        target       = 'geo_A' + cntynum + '_V1'

        df00         = mydb.get_alias(original).read()#.head(100)
        df0          = output.get_alias(target).read()#.head(100)
        cputime.tick('Dataframe read')
        
        #%%----------check------------
       
        number_split            = check(cntynum, df00, df00['num'].tolist(), 'num_check')
        #number_split            = check(cntynum, df0, df0['num_tail'].tolist(), 'num_check')
        
        df0                     = get_df(df0, number_split)

        floor                   = check(cntynum, df0, df0['floor_1'].tolist(), 'floor_check')
        df0                     = get_df(df0, floor)
        
        #%%--------uniform------------
        
        #df0['road']             = uni(df0['road'].fillna(value='').tolist(), 'text_uniform')
        #df0['zone']             = uni(df0['zone'].fillna(value='').tolist(), 'text_uniform')
        #df0['lane']             = uni(df0['lane'].fillna(value='').tolist(), 'text_uniform')
        #df0['lane']             = uni(df0['lane'].fillna(value='').tolist(), 'full2half_uniform')
        #df0['alley']            = uni(df0['alley'].fillna(value='').tolist(), 'text_uniform')
        #df0['alley']            = uni(df0['alley'].fillna(value='').tolist(), 'full2half_uniform')

        #df0['num_1']            = uni(df0['num_1'].fillna(value='').tolist(), 'full2half_uniform')
        #df0['num_1']            = uni(df0['num_1'].fillna(value='').tolist(), 'alphabet_uniform')
        
        #df0['num_2']            = uni(df0['num_2'].fillna(value='').tolist(), 'full2half_uniform')
        #df0['num_2']            = uni(df0['num_2'].fillna(value='').tolist(), 'alphabet_uniform')
         
        df0['floor_1']            = uni(df0['floor_1'].fillna(value='').tolist(), 'full2half_uniform')
        df0['floor_1']            = uni(df0['floor_1'].fillna(value='').tolist(), 'alphabet_uniform')
        
        df0['floor_2']            = uni(df0['floor_2'].fillna(value='').tolist(), 'full2half_uniform')
        df0['floor_2']            = uni(df0['floor_2'].fillna(value='').tolist(), 'alphabet_uniform')
        
        
        #df0['nsg_addr_key']     = df0['road']+ df0['zone']+ df0['lane'] + df0['alley'] + '(' + df0['num_p']+','+ df0['num_1']+','+df0['num_2'] + ')'
        cputime.tick('Columns uniformed')
       
        #%%-------needed column-------
        need = df0[["fid", "nsg_addr_key", "geom", "origin_address", "cnty_code", "town_code", "lie", "lin", "road", "zone", "lane", "alley", "num_p", "num_1", "num_2", "floor_p","floor_1", "floor_2",'demand',"memo", "dc_json_report"]]
        cputime.tick('Checking done')
       
        #%%---------output------------

        output.get_alias('geo_A' + cntynum + '_V3' ).write(need)
