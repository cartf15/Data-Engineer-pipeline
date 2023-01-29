import pandas as pd
import pyodbc
import numpy as np
import importlib.util


class ConnectorODBC:
    
    def __init__(self):
        spec = importlib.util.spec_from_file_location("props.import_properties", "\\import_properties.py")
        cls = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls)
        props = cls.Properties.get_conf_by_section('SqlServerCredentialsDocumentation')
        self.user = props.get('mssql.username')
        self.pws = props.get('mssql.password')
        self.server = props.get('mssql.server')
        self.db= props.get('mssql.database')
        # self.server = "enertiagemini"
        # self.database = "Enertia_Gemini_M3"
        # self.username = "GeminiData_User"
        # self.password = "Gem1n!_us#r"
    
    def _open_connection(self, db):
        ctx = pyodbc.connect('DRIVER={ODBC DRIVER 17 for SQL Server};SERVER='
                            +self.server+';DATABASE='+db+';UID='+self.user+';PWD='+self.pws)

        return ctx

    def get_cursor(self,ctx):
        cs = ctx.cursor()
        return cs

    def close_connector(self, ctx, cs):
        try:
            cs.close()
            ctx.close()
        finally:
            cs.close()
            ctx.close()


class ControllerODBC:

    def __init__(self, db):
        self.odbc_init_ = ConnectorODBC()
        self.user = self.odbc_init_.user
        self.pws = self.odbc_init_.pws
        self.server = self.odbc_init_.server
        self.db = db
        self.conn = self.odbc_init_._open_connection(db)
        self.cur = self.conn.cursor()

#     def select(self, schema,table):
#         r = f"SELECT * FROM {str(table)};"
#         return r

    
    def get_SQL(self):
        r = f"""
                   Query
                
                
                """
        return r



# result=ControllerODBC.get_tables()


    def get_columns(selF):
        r = f"""query2"""
        return r

    def add_table(self, df, table_name):
        for index, row in df.iterrows():
            self.cur.execute(f"INSERT INTO DGO_DEV.DOCUMENTATION.{table_name} values({row[0]},{row[1]})")

        self.conn.commit()

        # success, nchunks, nrows, _ = pd_sf.write_pandas(self.conn, df, table_name)
        # return(success, nchunks, nrows, _)


    def execute(self, sql_sentences):
        #Get DATAFRAME
        # exc = self.cur.execute(sql_sentences)
        df = pd.read_sql(sql_sentences,self.conn)
        return df
    
        