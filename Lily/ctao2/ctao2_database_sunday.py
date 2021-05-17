import os
import math
import pandas
from datetime import datetime
from multiprocessing import Pool
from Lily.ctao2.ctao2_nsgstring import alnum_uuid
from Lily.ctao2.ctao2_database import database
from Lily.ctao2.ctao2_database_mediator import manidb
from Lily.ctao2.ctao2_database_mediator import pure_memory_database
from Lily.ctao2.ctao2_hostmetadata import hostmetadata

###########################################
# just   for ctao2_release
class ctao2_database_sunday:
    __file__ = __file__

###########################################


def calc_leftjoin_operator (arg):
    database_name   = arg[0]
    left_table      = arg[1]   
    right_table     = arg[2]
    on_condition    = arg[3]
    calc_columns    = arg[4]

    memodb  = database(':memory:')
    filedb  = database(database_name)
    filedb.connect.backup(memodb.connect, pages=20, sleep=0.0001)
    memodb.connect.execute( '''drop TABLE if exists {0}_leftjoin_{1}'''.format(left_table, right_table) )
    memodb.connect.execute( '''create TABLE if not exists leftjoin_{0}_{1} as select L_key, R_key {3}
                               from {0} a left join  {1} b on {2}'''. format(left_table, right_table, on_condition, calc_columns) )
    memodb.connect.backup(filedb.connect, pages=20, sleep=0.0001)
    del filedb
    del memodb

#  
#  left_table (nsg_key primary key, geom)
#  right_table (nsg_key primary key, geom) 
#
class sunday(manidb):

    def __init__(self, sourcedb_filename):
        self.host   = hostmetadata()
        self.uuid   = alnum_uuid()[-6:]
        self.date   = datetime.now().strftime('%Y%m%d')
        self.tbeg   = datetime.now()

        self.metadata_table = 'pylily_sunday_leftjoin_geom'
        self.bufferdb_directory = '{0}/{1}'.format(self.host.factory, self.metadata_table)

        if  not os.path.exists(self.bufferdb_directory) :
            os.mkdir(self.bufferdb_directory)

        bufferdb_filename = '{0}/{1}_{2}.sqlite'.format(self.bufferdb_directory, self.date, self.uuid[-6:]) 

        self.bufferdb_name, self.bufferdb_extension = os.path.splitext(bufferdb_filename)

        super().__init__(sourcedb_filename)
        self.uuid   = self.uuid[-6:]
        self.timer('Construction database({0}) Accompilished'.format( sourcedb_filename))

    def distribution_num(self, data_num, iter_num):
        if data_num <=0 or iter_num <=0:
            return [1,1, 0]

        elif data_num  >= (iter_num * iter_num):
            return [data_num // iter_num , iter_num , data_num % iter_num ]

        else :
            iter_num =  math.floor(math.sqrt(data_num))
            return [data_num // iter_num , iter_num , data_num % iter_num]

    def leftjoin(self, left_table, right_table, on_condition, calc_columns=[]) :

        self.leftjoin_step1_distribution_data(left_table, right_table, on_condition, calc_columns)
        self.leftjoin_step2_multiprocessing()
        self.leftjoin_step3_gather_and_save_data()
        self.leftjoin_step4_clear_tempdatafile()

    def leftjoin_alpha_test(self, left_table, right_table, on_condition, calc_columns) :
        leftviewname    = 'calc_{0}_left'.format(self.uuid)
        rightviewname   = 'calc_{0}_right'.format(self.uuid) 
        self.connect.execute('create view {0} as select * from {1} limit 5000'.format(leftviewname, left_table ))
        self.connect.execute('create view {0} as select * from {1} limit 5000'.format(rightviewname, right_table))

        left_table  = leftviewname
        right_table = rightviewname
        self.leftjoin_step1_distribution_data(left_table, right_table, on_condition, calc_columns)
        self.leftjoin_step2_multiprocessing()
        self.leftjoin_step3_gather_and_save_data()
        self.leftjoin_step4_clear_tempdatafile()


    def leftjoin_step1_distribution_data(self, left_table, right_table, on_condition, calc_columns) :
        number   = 8 
        self.cursor.execute( '''SELECT count()  FROM '{table}' '''.format (table=left_table) )
        row_num  = self.cursor.fetchone()[0]
        self.cursor.execute( '''SELECT count()  FROM '{table}' '''.format (table=right_table) )
        rig_num  = self.cursor.fetchone()[0]
        lgap     =  self.distribution_num(row_num , number)
        rgap     =  self.distribution_num(rig_num , number)
     
        self.databaselist = []
        for n in range(0,     lgap[1] if lgap[2]==0 else lgap[1]+1 ):
            for m in range(0, rgap[1] if rgap[2]==0 else rgap[1]+1 ):
                output_name = '{0}_L{1:0>4}_R{2:0>4}{3}'.format(self.bufferdb_name, n, m, self.bufferdb_extension)
                self.attach_database(output_name, 'odata')
                self.connect.execute( '''create TABLE odata.{0} as select nsg_key as L_key, geom L_geom from {0} limit {1} offset {2}'''.format (left_table , lgap[0], n*lgap[0]) )
                self.connect.execute( '''create TABLE odata.{0} as select nsg_key as R_key, geom R_geom from {0} limit {1} offset {2}'''.format (right_table, rgap[0], m*rgap[0]) )
                self.detach_database('odata')
                
                self.databaselist.append([output_name, left_table,right_table, on_condition, ' '.join(calc_columns) ] )
                print (output_name)

        headers = ['filename','left_table', 'right_table', 'on_condition', 'calc_columns']
        self.to_table(self.metadata_table, pandas.DataFrame(self.databaselist, columns=headers) )

    def leftjoin_step2_multiprocessing(self):
        self.databaselist = self.to_dataframe(self.metadata_table).values.tolist()
        mpPool          = Pool(self.host.cpu_code )
        content         = mpPool.map(calc_leftjoin_operator, self.databaselist)
        mpPool.close()
        return content

    def leftjoin_step3_gather_and_save_data(self):
        self.databaselist = self.to_dataframe(self.metadata_table).values.tolist()
        
        if len(self.databaselist) == 0 : return

        left_table        = self.databaselist[0][1]
        right_table       = self.databaselist[0][2]
        rdset_table       = 'leftjoin_{0}_{1}'.format(left_table, right_table)

        self.connect.execute('drop TABLE if exists {0}'.format(rdset_table) )
        for database_name in self.databaselist:
            self.attach_database(database_name[0], 'idata')
            self.connect.execute( 'create table if not exists {0} as select * from idata.{0} where 0'''.format(rdset_table) )
            self.connect.execute( 'insert into {0} select * from idata.{0} '.format(rdset_table) )
            self.connect.commit()
            self.detach_database('idata')
        
        self.connect.execute('ALTER TABLE {0} RENAME TO calc_{1}'.format (rdset_table, self.uuid) )
        self.connect.execute('create view calc_{1}_view as select  L_key , count() not_null_num from calc_{1} where R_key is not null group by L_key'.format (rdset_table, self.uuid) )

        #去掉重複的資料
        self.connect.execute('create table {0} as select DISTINCT * from calc_{1}'.format(rdset_table, self.uuid) )
        #去掉假的null值
        self.connect.execute('delete from  {0} where R_key is Null and L_key in (select L_key from calc_{1}_view where not_null_num > 0) '.format(rdset_table, self.uuid) )
        self.connect.execute('ALTER TABLE {0} RENAME TO calc_{1}_keys'.format (rdset_table, self.uuid) )

        #合併原有的 left table
        self.connect.execute('create table {0} as select a.* , b.R_key from {2} a left join calc_{1}_keys b on a.nsg_key = b.L_key'.format(rdset_table, self.uuid, left_table) )

        self.connect.commit()

    def leftjoin_step4_clear_tempdatafile(self):
        self.databaselist = self.to_dataframe(self.metadata_table).values.tolist()

        for db in  self.databaselist :
            if os.path.exists(db[0]) :
                os.remove(db[0]) 

    def rename_ljr(self, newtablename, newcolumnname ):
        self.databaselist = self.to_dataframe(self.metadata_table).values.tolist()
        if len(self.databaselist) == 0 : return
        left_table        = self.databaselist[0][1]
        right_table       = self.databaselist[0][2]
        rdset_table       = 'leftjoin_{0}_{1}'.format(left_table, right_table)

        dataframe         = pandas.read_sql('select * from {0}'.format(rdset_table), self.connect)
        
        dataframe         = dataframe.drop([newcolumnname], axis=1, errors='ignore')

        dataframe.rename( columns={'R_key':'{0}'.format(newcolumnname)}, inplace=True)

        dataframe.to_sql(newtablename, self.connect, if_exists='replace', index=False)

#if __name__ == '__main__':
#    thishost          = hostmetadata()
#    sourcedb_filename =  thishost.factory    + '/lite_tp_bldg__streetblock_lj.sqlite'
#    targetdb_filename =  thishost.factory    + '/lite_tp_bldg__streetblock_lj2.sqlite'

#    mani = sunday(sourcedb_filename)
#    mani.connect.execute('select initspatialmetadata();')

#    mani.timer('lj start')
    
#    mani.leftjoin('argu_ainfo', 'data_nsg_u0200_streetblock', 'within(L_geom, R_geom)', [])
#    mani.timer('lj accompilished')

#    mani.backup(targetdb_filename)
#    mani.timer()