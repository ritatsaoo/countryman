import re
import os
import shutil
import pandas
from datetime import datetime
from Lily.ctao2.ctao2_database import database
from Lily.ctao2.ctao2_nsgstring import alnum_uuid
from Lily.ctao2.ctao2_hostmetadata import hostmetadata


class tickwatch:

    def __init__(self):
        self.uuid               = alnum_uuid()
        self.host               = hostmetadata()
        self.begtime            = datetime.now()
        self.msgtime            = [['beg', self.begtime, 0]]            

    def tick(self, msg_text='tick'):
        time_point              = datetime.now()
        seconds = (time_point - self.begtime).seconds
        self.msgtime.append( [msg_text, time_point, seconds ] )
        print ( '{1}_({0:06})\t\t'.format(seconds, self.uuid[-12:]), msg_text)

class alias:
    def __init__(self, parent_database_connect, table_name):

        self.table_name         = table_name 
        self.parent_connect     = parent_database_connect
        self.watch              = tickwatch()

    def tick(self, text):
        self.watch.tick(text)

    def replace(self, dataframe):
        self.write(dataframe, 'replace')

    def append(self, dataframe):
        self.write(dataframe, 'append')

    def write(self, dataframe, if_exists_arg='replace'):
        dataframe.to_sql(self.table_name, self.parent_connect, if_exists=if_exists_arg, index = False)

    def write_with_index(self, dataframe):
        dataframe.to_sql(self.table_name, self.parent_connect, if_exists='replace', index = True)

    def read(self, arg_primkey = None):
        check_sql   = '''SELECT name FROM sqlite_master WHERE type in ('table', 'view') AND name='{0}' '''.format(self.table_name);
        check_df    = pandas.read_sql(check_sql, self.parent_connect,)
        dataframe   = pandas.DataFrame()

        if not check_df.empty :
            if arg_primkey is None:
                dataframe   = pandas.read_sql(f'select (rowid -1) as nsg_row_primkey, * from {self.table_name}', self.parent_connect, index_col='nsg_row_primkey')
            else:
                dataframe   = pandas.read_sql(f'select {arg_primkey}  as nsg_row_primkey, * from {self.table_name}', self.parent_connect, index_col='nsg_row_primkey')

        return dataframe

class manidb (database):

    def __init__(self,  sourcedb_name):
        self.host   = hostmetadata()
        self.uuid   = alnum_uuid()

        super().__init__(sourcedb_name) 

        self.timelog = alias (self.database_path, 'pylily_manidb_timelog')

    def transaction (self, SQLtransaction_list):
        try:
            for sql in SQLtransaction_list:
                self.connect.execute(sql)

            self.timelog.tick(f'{SQLtransaction_list[0]} commit')

        except self.connect.Error:
            
            self.connect.rollback()
            self.timelog.tick(f'{self.database_path} transaction connect.error') 

        except:
            self.connect.rollback()
            self.timelog.tick(f'{self.database_path} transaction un except error')

    def get_alias(self, table_name):
        return alias (self.connect, table_name)

    def timer(self, message='time_counter'):
        self.timelog.timer('message {0}'.format(message))

    #def __del__(self):
    #    self.connect.commit()
    #    self.connect.close()


    def backup(self):
        #backup database_target to current_working_directory self.cwd 
        basename = os.path.basename(self.database_path)
        backup_filename = '{0}/mani_backup_{1}_{2}_{3}'.format(self.host.factory, self.date, self.uuid[-8:], basename)

        targetdb = database(backup_filename)
        self.connect.backup(targetdb.connect, pages=20, sleep=0.0001)
        del targetdb

    def drop_tables(self, table_re_pattern_list):
        tables = self.tables()
        for name in tables.index:
            for pattern in table_re_pattern_list :
                if re.match(pattern, name) and tables.loc[name, 'type'] == 'table' :
                    print ('drop table ', name)
                    self.connect.execute ( '''drop TABLE if exists {0}'''.format (name) )

                elif re.match(pattern, name) and tables.loc[name, 'type'] == 'view' :
                    print ('drop view ', name)
                    self.connect.execute ( '''drop VIEW  if exists {0}'''.format (name) )

    def read_table(self, _tablename):
        return pandas.read_sql_query('select * from ' + _tablename , self.connect)

    def overwrite_table(self, _tablename, _dataframe):
        _dataframe.to_sql(_tablename, self.connect, if_exists='replace', index=True)

    def __enter__(self):
        return self
    
    def __exit__(self, type, msg, traceback):
        if type:
            print(msg)       
        self.connect.commit()
        return False

    def merge_todisk(self,  args):
        #arg_dict = {   
        #    'tableA'        : 'argu_graph_road_inktty',
        #    'tableB'        : 'ktty_all',
        #    'keyA'          : ['nsg_key'],
        #    'keyB'          : ['Nsg_key'],
        #    'colsA'         : ['rd4','rd5'],
        #    'colsB'         : ['Rd4','Level'],
        #    'table_target'  : 'argu_graph_road_inktty' 
        # }

        part_A = pandas.read_sql('select * from {0}'.format( args['tableA'] ), self.connect, index_col=args['keyA'])
        part_B = pandas.read_sql('select * from {0}'.format( args['tableB'] ), self.connect, index_col=args['keyB'])
        
        for colA, colB in zip (args['colsA'], args['colsB']):
            part_A[colA] = part_B[colB]

        part_A.to_sql(args['table_target'], self.connect, if_exists='replace', index=True) 

if __name__ == '__console__' or __name__ == '__main__' :
    ftab = factory_table()

    ftab.timer()

    print(ftab.host.factory)