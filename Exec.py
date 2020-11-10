# print('test')
# import ControllerODBC 
# # import ControllerODBC as Cont



# ini_cont=ControllerODBC() # instance
# ini_conn=Main.ConnectorODBC._open_connection("Enertia_Gemini_M3")




# r=ini_conn._open_connection(db)

# result=ini_cont.get_tables(db) 
# # import pyodbc
# # import pandas as pd

# cnxn = .connect(< db details here >)
# cursor = cnxn.cursor()
# script = """
#         SELECT * FROM my_table
#           """

#------------------------------------------------V2

import pandas as pd
import pyodbc
import numpy as np
import importlib.util


server="enertiagemini"
db="Enertia_Gemini_M3"
user="GeminiData_User"
pws="Gem1n!_us#r"


try: 

  ctx = pyodbc.connect('DRIVER={ODBC DRIVER 17 for SQL Server};SERVER='
                              +server+';DATABASE='+db+';UID='+user+';PWD='+pws)
  print('Conexion exitosa')
except: 

  print ('No se pudo conectar' )


cs = ctx.cursor()
print('cursor ok')

# r = f"""
#                     SELECT rcorp.HdrCode AS BatchCorpCode, rcorp.HdrName AS BatchCorpName, corp.HdrCode AS GLCorpCode, corp.HdrName AS GLCorpName, acct.HdrCode AS AcctCode, acct.HdrName AS AcctName, icorp.HdrCode AS GLICCorpCode,
#                   prop.HdrCode AS PropCode, prop.HdrName AS PropName, purch.HdrCode AS PurchCode, purch.HdrName AS PurchName, glTxn.OrigTxnRowTID, gl.GlDtlTID, gl.DtlCorpHID, gl.DtlAcctHID, gl.DtlBatchTID, gl.DtlBatchNo, gl.DtlProcessTID,
#                   gl.DtlBankTxnTID, gl.DtlChkDepNo, gl.DtlInvoiceNo, gl.DtlTxnType, gl.DtlTxnSrcCode, gl.DtlTxnDate, gl.DtlAcctDate, gl.DtlSvcDate, gl.DtlAfeCatCode, gl.DtlBillCatCode, gl.DtlAtrType, gl.DtlAtrCode, gl.DtlProdCode, gl.DtlProdCmpnt,
#                   gl.DtlProdDsgnCode, gl.DtlUomCode, gl.DtlTaxStateCode, gl.DtlIntTypeCode, gl.DtlSysIntCode, gl.DtlVol, gl.DtlDesc, gl.DtlVal, gl.DtlVendorHID, gl.DtlPayeeHID, gl.DtlPurchaserHID, gl.DtlOwnerHID, gl.DtlRemitterHID, gl.DtlAfeHID,
#                   gl.DtlPropHID, gl.DtlICCorpHID, gl.DtlFisPeriodClosingTID, gl.DtlDdaOwnerHID, gl.DtlDistrib, gl.DtlCurrTransHistSpecRate, gl.DtlEqDtlTID, gl.DtlTxnTypeUserDefCode,
#                   REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(gl.DtlDesc, CHAR(10), ''), CHAR(13), ''), CHAR(10), ''), CHAR(13), ''), CHAR(10), ''), CHAR(13), ''), CHAR(10), ''), CHAR(13), ''), CHAR(10), ''),
#                   CHAR(13), '') AS DtlDesc1
#                     FROM     glMasDtl AS gl INNER JOIN
#                   glMasDtlPostTxn AS glTxn ON gl.GlDtlTID = glTxn.GlDtlTID AND glTxn.OrigTxnTableName = 'rvTxnDtl' INNER JOIN
#                   rvTxnDtl AS dtl ON glTxn.OrigTxnRowTID = dtl.TxnDtlTID INNER JOIN
#                   rvTxnHdr AS hdr ON dtl.TxnHdrTID = hdr.TxnHdrTID INNER JOIN
#                   rvTxnChkHdr AS chk ON hdr.TxnChkTID = chk.TxnChkTID INNER JOIN
#                   aaMasBatch AS b ON chk.TxnBatchTID = b.BatchTID INNER JOIN
#                   fbMasHdr AS rcorp ON b.CorpHID = rcorp.HdrHID LEFT OUTER JOIN
#                   fbMasHdr AS corp ON gl.DtlCorpHID = corp.HdrHID LEFT OUTER JOIN
#                   fbMasHdr AS acct ON gl.DtlAcctHID = acct.HdrHID LEFT OUTER JOIN
#                   fbMasHdr AS icorp ON gl.DtlICCorpHID = icorp.HdrHID LEFT OUTER JOIN
#                   fbMasHdr AS purch ON gl.DtlPurchaserHID = purch.HdrHID INNER JOIN
#                   fbMasHdr AS prop ON dtl.TxnRevPropHID = prop.HdrHID
#                         WHERE  (hdr.TxnProdDate BETWEEN '1/1/2017' AND '1/31/2017') AND (prop.HdrCode NOT LIKE '%SOLD%')
#                         ORDER BY BatchCorpCode
#                         OFFSET 0 ROWS
#                         FETCH NEXT 10 ROWS ONLY
                
                
                # """

# data = pd.read_sql(r,ctx)
# print(data)
# writer = pd.ExcelWriter("C:\\Users\\ca6359\\Desktop\\Hilcorp\\Output Files\\test_10.xlsx")
# data.to_excel(writer, sheet_name='Sheet1')
# writer.save()
# writer.close()
# cs.close()
# ctx.close()



# with pd.ExcelWriter("C:\\Users\\ca6359\\Desktop\\Hilcorp\\Output Files\\test_10.xlsx") as writer:  
#   data.to_excel(writer, sheet_name='Sheet_name_1')
  

















