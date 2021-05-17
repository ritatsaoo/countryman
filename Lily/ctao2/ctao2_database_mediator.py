import re
import os
import shutil
import pandas
from datetime import datetime
from Lily.ctao2.ctao2_database import database
from Lily.ctao2.ctao2_nsgstring import alnum_uuid
from Lily.ctao2.ctao2_hostmetadata import hostmetadata

class table:
    def __init__(self,  database_path, table_name):
        self.uuid               = alnum_uuid()
        self.host               = hostmetadata()
        self.begtime            = datetime.now()
        self.msgtime            = [['beg', self.begtime, 0]]
        self.table_name         = table_name 
        self.database_path      = database_path        
            
    def replace(self, dataframe):
        self.write(dataframe, 'replace')

    def append(self, dataframe):
        self.write(dataframe, 'append')

    def write(self, dataframe, if_exists_arg='replace'):
        targetdb    = manidb(self.database_path)
        dataframe.to_sql(self.table_name, targetdb.connect, if_exists=if_exists_arg, index = False)
        del targetdb

    def write_with_index(self, dataframe):
        targetdb    = manidb(self.database_path)
        dataframe.to_sql(self.table_name, targetdb.connect, if_exists='replace', index = True)
        del targetdb

    def read(self, arg_primkey='rowid'):
        targetdb    = database(self.database_path)
        
        check_sql   = '''SELECT name FROM sqlite_master WHERE type in ('table', 'view') AND name='{0}' '''.format(self.table_name);
        check_df    = pandas.read_sql(check_sql, targetdb.connect)
        
        dataframe   = pandas.DataFrame()

        if not check_df.empty :
            dataframe   = pandas.read_sql(f'select {arg_primkey} as nsg_row_primkey, * from {self.table_name}', targetdb.connect, index_col='nsg_row_primkey')

        del targetdb
        return dataframe

    def timer(self, message='touch'):
        time_point              = datetime.now()
        self.msgtime.append( [message, time_point, (time_point - self.begtime).seconds ] )
        return time_point

    def __del__(self):
        this_table = 'DEL {0}\n\t\t\t\t\t\t\tTABLE->{1}'.format(self.database_path, self.table_name)
        self.timer( this_table )
        self.tick ( this_table )

    def tick(self, text):
        time_point              = datetime.now()
        seconds = (time_point - self.begtime).seconds  
        print ( '{1}_TICK SECOND({0:06})\t\t'.format(seconds, self.uuid[-12:]), text, time_point)

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
        
    def replace(self, dataframe):
        self.write(dataframe, 'replace')

    def append(self, dataframe):
        self.write(dataframe, 'append')

    def write(self, dataframe, if_exists_arg='replace'):
        dataframe.to_sql(self.table_name, self.parent_connect, if_exists=if_exists_arg, index = False)

    def write_with_index(self, dataframe):
        dataframe.to_sql(self.table_name, self.parent_connect, if_exists='replace', index = True)

    def read(self, arg_primkey='rowid'):
        check_sql   = '''SELECT name FROM sqlite_master WHERE type in ('table', 'view') AND name='{0}' '''.format(self.table_name);
        check_df    = pandas.read_sql(check_sql, self.parent_connect,)
        dataframe   = pandas.DataFrame()

        if not check_df.empty :
            dataframe   = pandas.read_sql(f'select {arg_primkey} as nsg_row_primkey, * from {self.table_name}', self.parent_connect, index_col='nsg_row_primkey')

        return dataframe

class manidb (database):

    def __init__(self,  sourcedb_name):
        self.host   = hostmetadata()
        self.uuid   = alnum_uuid()

        super().__init__(sourcedb_name) 

        self.timelog = table (self.database_path, 'pylily_manidb_timelog')

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

    def get_table(self, table_name):
        self.timelog.timer('create alias {0}'.format(table_name))
        alias = table (self.database_path, table_name)
        return alias

    def get_alias(self, table_name):
        return alias (self.connect, table_name)

    def timer(self, message='time_counter'):
        self.timelog.timer('message {0}'.format(message))

    def __del__(self):
        self.connect.commit()
        self.connect.close()
        self.timelog.timer(self.database_path)

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


class pure_memory_database (manidb):
    def __init__(self,  sourcedb_path):

        self.host           = hostmetadata()
        self.uuid           = alnum_uuid()
        self.date           = datetime.now().strftime('%Y%m%d')
        self.tbeg           = datetime.now()
        self.sourcedb_path  = sourcedb_path

        super().__init__(':memory:') 
        source = database(sourcedb_path)        
        source.connect.backup(self.connect, pages=20, sleep=0.0001)       
        del source

    def dump_table_todisk(self, targetdb_filename, table_name):     
        self.attach_database(targetdb_filename, 'odata')

        self.connect.execute('drop table if exists odata.{0}'.format(table_name) )
        self.connect.execute('create table  odata.{0} as select * from {0}'.format(table_name) )

        self.detach_database('odata')

    def clone_table_fromdisk(self, datafile, table_name):
        source      = manidb(datafile)
    
        db_table    = source.read_table(table_name)
        
        self.overwrite_table(table_name, db_table)
        del source



    def backup(self):
        #backup database_target to current_working_directory self.cwd 
        basename = os.path.basename(self.sourcedb_path)
        backup_filename = '{0}/mani_backup_{1}_{2}_{3}'.format(self.host.factory, self.date, self.uuid[-8:], basename)

        targetdb = database(backup_filename)
        self.connect.backup(targetdb.connect, pages=20, sleep=0.0001)
        del targetdb



class factory_table (table) :
    def __init__(self,  table_name='data_pylily_factory'):
        super().__init__('none', table_name)
        self.database_path   = '{0}/pylily_ctao2_pool.sqlite'.format(self.host.factory)        
        self.metatable       = table(self.database_path, 'data_pylily_timecost')     

    def timer(self, message='factory_touch'):
        super().timer(message)
        header      = ['uuid', 'table', 'message', 'time_point']
        messag      = [self.uuid, self.table_name, message, self.time[message] ]
        dataframe   = pandas.DataFrame([messag], columns=header)
        self.metatable.append(dataframe)
 

if __name__ == '__console__' or __name__ == '__main__' :
    ftab = factory_table()

    ftab.timer()

    print(ftab.host.factory)