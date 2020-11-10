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
                    SELECT rcorp.HdrCode AS BatchCorpCode, rcorp.HdrName AS BatchCorpName, corp.HdrCode AS GLCorpCode, corp.HdrName AS GLCorpName, acct.HdrCode AS AcctCode, acct.HdrName AS AcctName, icorp.HdrCode AS GLICCorpCode,
                  prop.HdrCode AS PropCode, prop.HdrName AS PropName, purch.HdrCode AS PurchCode, purch.HdrName AS PurchName, glTxn.OrigTxnRowTID, gl.GlDtlTID, gl.DtlCorpHID, gl.DtlAcctHID, gl.DtlBatchTID, gl.DtlBatchNo, gl.DtlProcessTID,
                  gl.DtlBankTxnTID, gl.DtlChkDepNo, gl.DtlInvoiceNo, gl.DtlTxnType, gl.DtlTxnSrcCode, gl.DtlTxnDate, gl.DtlAcctDate, gl.DtlSvcDate, gl.DtlAfeCatCode, gl.DtlBillCatCode, gl.DtlAtrType, gl.DtlAtrCode, gl.DtlProdCode, gl.DtlProdCmpnt,
                  gl.DtlProdDsgnCode, gl.DtlUomCode, gl.DtlTaxStateCode, gl.DtlIntTypeCode, gl.DtlSysIntCode, gl.DtlVol, gl.DtlDesc, gl.DtlVal, gl.DtlVendorHID, gl.DtlPayeeHID, gl.DtlPurchaserHID, gl.DtlOwnerHID, gl.DtlRemitterHID, gl.DtlAfeHID,
                  gl.DtlPropHID, gl.DtlICCorpHID, gl.DtlFisPeriodClosingTID, gl.DtlDdaOwnerHID, gl.DtlDistrib, gl.DtlCurrTransHistSpecRate, gl.DtlEqDtlTID, gl.DtlTxnTypeUserDefCode,
                  REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(gl.DtlDesc, CHAR(10), ''), CHAR(13), ''), CHAR(10), ''), CHAR(13), ''), CHAR(10), ''), CHAR(13), ''), CHAR(10), ''), CHAR(13), ''), CHAR(10), ''),
                  CHAR(13), '') AS DtlDesc1
                    FROM     glMasDtl AS gl INNER JOIN
                  glMasDtlPostTxn AS glTxn ON gl.GlDtlTID = glTxn.GlDtlTID AND glTxn.OrigTxnTableName = 'rvTxnDtl' INNER JOIN
                  rvTxnDtl AS dtl ON glTxn.OrigTxnRowTID = dtl.TxnDtlTID INNER JOIN
                  rvTxnHdr AS hdr ON dtl.TxnHdrTID = hdr.TxnHdrTID INNER JOIN
                  rvTxnChkHdr AS chk ON hdr.TxnChkTID = chk.TxnChkTID INNER JOIN
                  aaMasBatch AS b ON chk.TxnBatchTID = b.BatchTID INNER JOIN
                  fbMasHdr AS rcorp ON b.CorpHID = rcorp.HdrHID LEFT OUTER JOIN
                  fbMasHdr AS corp ON gl.DtlCorpHID = corp.HdrHID LEFT OUTER JOIN
                  fbMasHdr AS acct ON gl.DtlAcctHID = acct.HdrHID LEFT OUTER JOIN
                  fbMasHdr AS icorp ON gl.DtlICCorpHID = icorp.HdrHID LEFT OUTER JOIN
                  fbMasHdr AS purch ON gl.DtlPurchaserHID = purch.HdrHID INNER JOIN
                  fbMasHdr AS prop ON dtl.TxnRevPropHID = prop.HdrHID
                        WHERE  (hdr.TxnProdDate BETWEEN '1/1/2017' AND '1/31/2017') AND (prop.HdrCode NOT LIKE '%SOLD%')
                        ORDER BY BatchCorpCode
                        OFFSET 0 ROWS
                        FETCH NEXT 100000 ROWS ONLY
                
                
                """
        return r



# result=ControllerODBC.get_tables()


    def get_columns(selF):
        r = f"""select t.name TABLE_NAME, c.name COLUMN_NAME, ty.name DATA_TYPE from sys.tables t 
                inner join sys.columns c on t.object_id = c.object_id
                inner join sys.types ty on c.system_type_id = ty.system_type_id
                ORDER BY 1;"""
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
    
        