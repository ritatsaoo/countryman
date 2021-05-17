import re
import pandas

from Lily.ctao2.ctao2_42    import answer
from Lily.ctao2.ctao2_42    import answer
from Lily.ctao2.ctao2_42    import numbering_list
from Lily.ctao2.ctao2_42    import split_list_to_chunks

from Lily.ctao2.ctao2_postgres_alias import postgresql
from Lily.ctao2.ctao2_database_alias import manidb



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

        ## symbol tranform ##
        self.num_chn= r'^[一二三四五六七八九十零壹貳參肆伍陸柒捌玖拾兩千仟百佰]{1,5}$'
        self.num_xx0= r'(^[十拾]$)'
        self.num_xx1= r'(^[一二三四五六七八九壹貳參肆伍陸柒捌玖]$)'
        self.num_xx2= r'(^[十拾])([一二三四五六七八九壹貳參肆伍陸柒捌玖]$)'
        self.num_xx3= r'(^[一二三四五六七八九壹貳參肆伍陸柒捌玖])([十拾]$)'
        self.num_xx4= r'(^[一二三四五六七八九壹貳參肆伍陸柒捌玖])([十拾])([一二三四五六七八九壹貳參肆伍陸柒捌玖]$)'
        self.num_xh1= r'(^[一二三四五六七八九壹貳參肆伍陸柒捌玖])([百佰])$'
        self.num_xh2= r'(^[一二三四五六七八九壹貳參肆伍陸柒捌玖])([百佰])(零)([一二三四五六七八九壹貳參肆伍陸柒捌玖])$'
        self.num_xh3= r'(^[一二三四五六七八九壹貳參肆伍陸柒捌玖])([百佰])([一二三四五六七八九壹貳參肆伍陸柒捌玖])([十拾])$'
        self.num_xh4= r'(^[一二三四五六七八九壹貳參肆伍陸柒捌玖])([百佰])([一二三四五六七八九壹貳參肆伍陸柒捌玖])([十拾])([一二三四五六七八九壹貳參肆伍陸柒捌玖])$'
        
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

        
        if not re.match(self.num_chn, addr_text):
            return addr_text

        if re.match(self.num_xx0, addr_text):
            addr_text = '10'

        elif re.match(self.num_xx2, addr_text):
            addr_text = re.sub(self.num_xx2, '1\g<2>', addr_text)

        elif re.match(self.num_xx3, addr_text):
            addr_text = re.sub(self.num_xx3, '\g<1>0', addr_text)

        elif re.match(self.num_xx4, addr_text):
            addr_text = re.sub(self.num_xx4, '\g<1>\g<3>', addr_text)

        elif re.match(self.num_xh1, addr_text):
            addr_text = re.sub(self.num_xh1, '\g<1>00', addr_text)

        elif re.match(self.num_xh2, addr_text):
            addr_text = re.sub(self.num_xh2, '\g<1>0\g<4>', addr_text)

        elif re.match(self.num_xh3, addr_text):
            addr_text = re.sub(self.num_xh3, '\g<1>\g<3>0', addr_text)

        elif re.match(self.num_xh4, addr_text):
            addr_text = re.sub(self.num_xh4, '\g<1>\g<3>\g<5>', addr_text)

        return addr_text.translate(self.uni_full2half_list)



class ax_htax_decomposition:
    def __init__(self):
        ##
        '''initial '''
        self.rep = {}

        ##
        ## Origin Reqular expression pattern for NUMBER/ CITY/ ZONE/ LIE 
        self.rep['town']        = r'([\u4e00-\u9fa5]{1,4}[區鄉市鎮])'
        self.rep['tract']       = r'([\u4e00-\u9fa5]{1,4}[里村])'
        self.rep['lin']         = r'([0-9０-９一二三四五六七八九十○]{1,4}鄰)'

        ##
        ## for ROAD (street)        
        self.rep['road']        = r'([\u4e00-\u9fa5]{1,4}(?:(?:路)|(?:大道)|(?:街))(?:[0-9０-９ㄧ一二三四五六七八九十]段)?)'

        ## 
        ##reqular expression pattern for LAN (巷)
        ##BUGFIX 可能存在 [十] 的數字漏洞
        LANE1       = r'(?:[0-9０-９一二三四五六七八九十○]{1,4}巷)'
        LANE2       = r'(?:[\u4e00-\u9fa5]{1,4}巷)'

        self.rep['lane']        = f'({LANE1}|{LANE2})'

        ##
        ##reqular expression pattern for ALLEY (弄)
        ##BUGFIX 可能存在 [十] 的數字漏洞
        ALLEY1      = r'(?:[0-9０-９一二三四五六七八九十○]{1,4}弄)'
        ALLEY2      = r'(?:[\u4e00-\u9fa5]{1,4}弄)'

        self.rep['alley']       = f'({ALLEY1}|{ALLEY2})'

        ##
        #class1  of address regulaer expression
        SEGM1       = f'''{self.rep["town"]}?{self.rep["tract"]}?{self.rep["lin"]}?'''
        SEGM2       = f'''{self.rep["road"]}{self.rep["lane"]}?{self.rep["alley"]}?'''
        self.rep1   = f'''^{SEGM1}{SEGM2}'''

        ##reqular expression pattern for (號/樓) 
        ## combination regular expression



        NUMP        = r'([臨近]?)'
        NUM_        = r'([\-]{1}[0-9]{1,4})?'
        NUMd        = r'((?:[0-9]{1,4})(?:[\-]{1}[0-9]{1,4})?)'
        self.num1   = f'''^{NUMP}{NUMd}號{NUM_}'''

        NUMcm       = r'(,(?:[0-9]{1,4})(?:[\-]{1}[0-9]{1,4})?)*'
        self.num2   = f'''^{NUMd}{NUMcm}號'''

        self.num_garble   = r'''號\-[0-9]{1-5}樓'''

        self.floor_g = r'''^([0-9]{1,4})樓'''
        self.floor_b = r'''^(?:房屋)?地下([0-9]{0,4})[樓層室]?'''


    def decomposition(self, arg_item ):
        import json
        #
        #gobj

        ind, addr_text       = arg_item
        ax_unitext_gobj_1    = countryman4uniform() 
        target_result = {}

        dc_report = {}
        #
        #        

        def step1 (addr : str):
            #decomposition (town, tract, road) 鄉鎮區/村里/路巷弄
            #critical operation            
            target_result ['addr']  = addr_text

            repattern   = re.compile(self.rep1)
            match       = re.match(repattern, addr)

            if match:
                dc_report['re']=True

                prefix      = match.group(0)
                mglist      = [ str(x or '') for x in match.groups() ]
                target_result ['town']  = mglist[0]
                target_result ['tract'] = mglist[1]
                target_result ['road']  = ax_unitext_gobj_1.number2txt_uniform(mglist[3])
                target_result ['lane']  = ax_unitext_gobj_1.txt2number_uniform(mglist[4])
                target_result ['alley'] = ax_unitext_gobj_1.txt2number_uniform(mglist[5])
                target_result ['dc_unusual_tail']  = addr[len(prefix):]
            else:
                dc_report['re']=False

                target_result ['dc_unusual_tail'] = addr

        #
        #
        def step2 (addr_num: str):
            #decomposition (number) 門牌號碼
            #initial arguments
            #中文[數字]處理
            #
            while True:
                match1  = re.search(ax_unitext_gobj_1.num_chn, addr_num)

                if match1 is None:
                    break

                ind1 = match1.regs[0][0]
                ind2 = match1.regs[0][1]
                nnum = ax_unitext_gobj_1.txt2number_uniform(  addr_num[ind1:ind2] )
                addr_num  = addr_num[:ind1] + nnum + addr_num [ind2:] 

            addr_num     = ax_unitext_gobj_1.full2half_uniform(addr_num)
            addr_num     = ax_unitext_gobj_1.symbol_uniform(addr_num)

            print (addr_num)

            #
            #critical operation
            clist        = re.findall(self.num_garble, addr_num)
            match_one    = re.match(self.num1, addr_num )
            match_two    = re.match(self.num2, addr_num )

            if len(clist) != 0:
                dc_report['num_tp'] = False

            elif match_one:
                dc_report['num_tp'] = True 

                mglist      = [ str(x or '') for x in match_one.groups()]
                target_result['num_p'] = mglist[0]
                target_result['num_1'] = mglist[1]
                target_result['num_2'] = mglist[2]
                target_result['num_head'] = match_one.group(0)
                target_result['num_tail'] = addr_num[len(target_result['num_head']):]

            elif match_two:
                dc_report['num_tp'] = True

                mglist      = [ str(x or '') for x in match_two.groups()]
                target_result['num_p'] = ''
                target_result['num_1'] = mglist[0]
                target_result['num_2'] = ''
                target_result['num_head'] = match_two.group(0)
                target_result['num_tail'] = addr_num[len(target_result['num_head']):]

            else:
                dc_report['num_tp'] = False
        #
        #
        def step3 ():
            #decomposition (floor) 樓層數
            #initial arguments
            match_g = re.match(self.floor_g, target_result['num_tail'])
            match_b = re.match(self.floor_b, target_result['num_tail'])
            if match_g:
                dc_report['floor']      = 'G'

                target_result['floor']  =  int(match_g.groups()[0])

            elif target_result['num_tail'] == '':
                dc_report['floor']      = 'G'

                target_result['floor']  = 1

            elif match_b:
                dc_report['floor'] = 'B'

                mglist      = [ str(x or '') for x in match_b.groups()]
                target_result['floor'] = -1 if mglist == [''] else -1 * int(match_b.groups()[0])

            else:
                dc_report['floor'] = ''
                target_result['floor'] =  1

            target_result['dc_json_report']  = json.dumps(dc_report)

        #
        #
        step1 (addr_text)
        step2 (target_result['dc_unusual_tail']) if dc_report['re'] else {}
        step3 () if dc_report['re'] and dc_report['num_tp']  else {}
  

        return (ind, target_result)

if __name__ == '__console__' or __name__ == '__main__':
    #machine argument

    uAnswer     = answer()
    ax_dec_1    = ax_htax_decomposition()
    workdir     = uAnswer.host.home + '/Desktop/crying_freeman/data_nsg/' 
    origindb    = manidb( workdir + 'nsg_bldg_TTK_bm.sqlite' ) 
    target_tab  = '10017'

    #read data
    df_all      = origindb.get_alias(f'rawdata_hou_A{target_tab}_addr').read()

    #for debug and development 
    #df_bin                  = df_all.head(10000)

    df_bin      = df_all
    df_bin      = df_bin.set_index(['HOU_LOSN'])

    #df_bin['LOCAT_ADDR']    = df_bin['LOCAT_ADDR'].apply( lambda x :  x.split() [0] )

    ##  decomposition 
    ##
    onere       = [ ax_dec_1.decomposition (item) for item in df_bin['LOCAT_ADDR' ].items() ]

    df_onere    = pandas.DataFrame.from_dict(dict(onere), orient='index')
    df_onere    = df_onere.sort_index()
    df_onere.index.name = 'HOU_LOSN'

    # select_cols = ['addr', 'town', 'tract', 'road', 'lane', 'alley', 'num_p', 'num_1', 'num_2', 'floor', 'num_head', 'num_tail', 'dc_unusual_tail', 'dc_json_report']
    # df_onere    = df_onere[ select_cols ]

    origindb.get_alias(f'mineral_A{target_tab}').write_with_index(df_onere)
    print(uAnswer.tick())