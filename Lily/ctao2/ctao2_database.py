#//# coding=utf-8
import os
import sqlite3
import pandas
import numpy
import os, socket, platform, datetime

class database:

    def __init__(self , database_path):

        self.database_path      = database_path
        self.connect            = sqlite3.connect(database_path)  
        self.connect.enable_load_extension(True)

        self.platform   = platform.platform()[:6]  

        try:
            if self.platform == 'Window':
                self.connect.load_extension('mod_spatialite') 
            else:
                self.connect.load_extension('libspatialite')

        except sqlite3.Error as e:
            print('''An error occurred:''', e.args[0])

        self.cursor             = self.connect.cursor()
        self.alias_count        = 0

    def __del__(self):
        self.connect.commit()
        self.connect.close()

    def rename_table (self, src_table, tar_table):
        self.cursor.execute ( '''ALTER TABLE {0}  RENAME TO {1}'''.format (src_table, tar_table) )
        return 


    def recover_geometry_column(self, table, geom_column, geom_type, coord_dimension, srid = 3826,  spatial_index_enabled = 0):
        #TODO
        pd_gc = pandas.read_sql_query('''select  * from 'geometry_columns' ''', self.connect, index_col=['f_table_name'])
        pd_gc.srid.astype(numpy.int64)

        pd_gc.at[table] = (geom_column, geom_type, coord_dimension, srid, spatial_index_enabled )

        pd_gc.to_sql('geometry_columns', self.connect, if_exists='replace', index=True)

        self.connect.commit()

    def CloneGeometryTable(self, oldname, newname, geometrycolumn = 'geom'):
        self.clone_table(oldname, newname)
        self.RecoverGeometryColumn(newname, geometrycolumn)
        self.connect.commit()

    def RecoverGeometryColumn(self, _table, _geom='geom'):
        pd_geom = self.check_geometry(_table, _geom)

        if pd_geom.srid[0] is None or pd_geom.type[0] is None:
            return

        if pd_geom['num'].idxmax() != pd_geom['num'].idxmin():
            return

        row=pd_geom.iloc[ pd_geom['num'].idxmax() ]
        
        sql = '''SELECT RecoverGeometryColumn
                ('{table}', '{geom}', {srid}, '{type}', 
                '{coord_dimenstion}' );'''.format(table=_table, geom=_geom, srid=row['srid'], type=row['type'], coord_dimenstion=row['coord_dimenstion'])

        self.cursor.execute (sql)
        self.connect.commit()

    def DiscardGeometryColumn(self, _table, _geom):
        sql = '''SELECT DiscardGeometryColumn
                ('{table}', '{geom}');'''.format(table=_table, geom=_geom)

        self.cursor.execute (sql)
        self.connect.commit()

    def clone_table (self, src_table, tar_table):
        # read schema and clone data structure(table)
        sql_schema = '''SELECT sql FROM sqlite_master WHERE type='table' AND name='{table}' '''.format(table=src_table)
        _dataframe = pandas.read_sql(sql_schema, self.connect)
        
        if _dataframe.empty :
            return

        self.drop_table(tar_table)
        sql = _dataframe.iloc[0]['sql'].replace(src_table, tar_table)
        self.cursor.execute (sql)

        # clone data to target table
        sql_schema = '''insert into {target} select * from {source}; '''.format( target=tar_table,  source = src_table)
        self.cursor.execute (sql_schema)
        self.connect.commit()
        return 

    def clone_database (self, re_parttern, output_db):
        import re
        if not output_db.isgeomdb():
            output_db .cursor.execute ('select initspatialmetadata();')

        for table_name in self.tables().index:
            if re.match(re_parttern, table_name):
                print (table_name)

                db_table = pandas.read_sql( '''SELECT * from {0} ''' .format(table_name), self.connect)

                output_db.to_table(table_name, db_table )
        #        output_db.RecoverGeometryColumn(table_name, 'geom')
        return 
#
# alter /modify 
#
    def drop_table (self, _table):
        self.cursor.execute ( '''drop TABLE if exists {0}'''.format (_table) )
        return 

    def vacuum (self):
        
        self.connect.execute( '''delete from geometry_columns where f_table_name not in (select name from sqlite_master)''' )
        self.connect.commit()
        self.connect.execute ( '''vacuum''' )
        return 

    def attach_database (self, database_filename , database_alias ):
        self.alias_count = self.alias_count + 1 
        comm = '''attach database '{0}' as {1} ''' . format (database_filename,  database_alias ) ;
        self.connect.execute( comm )

    def detach_database (self, database_alias ):
        self.alias_count = self.alias_count + 1 
        comm = '''detach database '{0}' ''' . format ( database_alias ) ;
        self.connect.execute( comm )
#
#   excel file
#

    def fit_xlsx(self, dataframe):
            #dtype.num
            #A unique number for each of the 21 different built-in types.
            # 19 str
            # 12 float
        if dataframe.shape[0] <= 1012345 and dataframe.shape[1]  <= 6123 :
            for col, dtype in zip(dataframe.columns, dataframe.dtypes):
                print (col, dtype, dtype.num)
                if  dtype.num == 17 : 
                    dataframe[col] = dataframe[col].apply(lambda x : x if  isinstance(x, str) and len(x) < 3000 else None)

            return dataframe
        else:
            return pandas.DataFrame()


    def to_xlsx (self, tablename_re_parttern='(argu_|calc_|data_).*', xlsx_filename=None):
        import re, pandas
        from Lily.blacksmith.file_feature import get_feature

        if xlsx_filename is None:
            feature = get_feature(self.database_path)
            xlsx_filename = feature['path'] + '/' + feature['name'] + '_lily.xlsx'

        writer    = pandas.ExcelWriter(xlsx_filename, engine = 'xlsxwriter')

        for table_name in self.tables().index:

            if re.match(tablename_re_parttern, table_name) and len(table_name) < 32:
                dataframe = self.to_dataframe(table_name) 
                dataframe = self.fit_xlsx(dataframe)
                if dataframe.empty is not True:
                    dataframe.to_excel(writer, index_label='pd_idx', sheet_name=table_name)

        writer.save()
        writer.close()
        return 
        
    def update_from_xlsx (self, xlsx_filename=None):
        import re, pandas
        from Lily.blacksmith.file_feature import get_feature

        if xlsx_filename is None:
            feature = get_feature(self.database_path)
            xlsx_filename = feature['path'] + '/' + feature['name'] + '_lily.xlsx'

        ddict = pandas.read_excel(xlsx_filename, sheet_name=None, index_col=0)

        for tablename in ddict:
            dataframe_1 = ddict[tablename]
            self.to_table(tablename + '_lily_xlsx', dataframe_1)

        return dict
#
#   dataframe <--> table
#
    def to_dataframe(self, _tablename):
        coltype = self.table_info(_tablename)
        ngeo    = coltype.geomtype.isnull()
        geom    = coltype.geomtype.notnull()
        col1    = [colname for colname in coltype[ngeo].index]
        col2    = ['asewkt({0}) as {0}'.format(colname) for colname in coltype[geom].index]
        
        col1.extend(col2)
        _col = ' ,'.join( col1 )

        return pandas.read_sql_query('''select {0} from {1} '''.format(_col, _tablename), self.connect)

    def to_table(self, _tablename, _dataframe):
        _dataframe.to_sql(_tablename, self.connect, if_exists='replace', index=False)

        #
        #if 'geom' in _dataframe.columns:

        #    self.cursor.execute ( '''update {tab} set geom = geomfromewkt(geom) '''.format(tab=_tablename) )
        #    self.RecoverGeometryColumn(_tablename, 'geom')
            



    def to_table_with_index(self, _tablename, _dataframe):
        _dataframe.to_sql(_tablename, self.connect, if_exists='replace', index=True)
#
#   function for getting shape of database
#
    def tables (self):
        return pandas.read_sql_query('''select  name, type, sql from sqlite_master where type in ('table', 'view') ''', self.connect, index_col=['name'])

    def check_geometry (self, _table, _geom):
        sql = '''SELECT cast(Count(*) as integer) num, GeometryType("{1}") type, cast(Srid("{1}") as integer) srid , 
                 CoordDimension("{1}") coord_dimenstion FROM "{0}"
                 GROUP BY 2, 3, 4'''.format (_table, _geom)

        return pandas.read_sql(sql, self.connect)

    def table_info(self, _table):
        dataframe_info = pandas.read_sql_query('''pragma table_info('{0}')'''.format(_table), self.connect, index_col=['name'])

        for column_name in dataframe_info.index:
            pd_geom = self.check_geometry(_table, column_name)
            row=pd_geom.iloc[ pd_geom['num'].idxmax() ]

            dataframe_info.loc[column_name, 'srid'] = row['srid']
            dataframe_info.loc[column_name, 'geomtype'] = row['type']
            dataframe_info.loc[column_name, 'num']  = row['num']
            dataframe_info.loc[column_name, 'CoorDimension'] = row['coord_dimenstion']
        return dataframe_info 

    def check_database (self, re_parttern = '(argu_|calc_|data_).*'):
        import re

        tablelist = [name for name in self.tables().index if re.match(re_parttern, name)]
        tablelist.sort()

        for name in tablelist:
            print (name)

        return tablelist


    def isgeomdb(self):
        return True if 'spatial_ref_sys' in self.tables().index else False
###########################################