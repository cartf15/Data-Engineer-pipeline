"""
This code is used to pull 200 millions of records from a database to a size delimited flat files.
The purpose of this code is migrate Enertia ERP system Data to SAP.

"""


c_query="""
                with cte as 
                (
                SELECT DISTINCT wc.HdrHID
                FROM fbMasHdr wc
                INNER JOIN fbMasHdrRelation r1 ON wc.HdrHID = r1.HdrRelChildHID AND r1.HdrRelRuleTID = 1000238
                INNER JOIN fbMasHdrRelation r2 ON r1.HdrRelParentHID = r2.HdrRelChildHID AND r2.HdrRelRuleTID = 1000177
                INNER JOIN fbMasHdrRelation r3 ON r2.HdrRelParentHID = r3.HdrRelChildHID AND r3.HdrRelRuleTID = 1000176
                INNER JOIN fbMasHdrRelation r4 ON r3.HdrRelParentHID = r4.HdrRelChildHID AND r4.HdrRelRuleTID = 1000175
                INNER JOIN fbMasHdrRelation r5 ON r4.HdrRelParentHID = r5.HdrRelChildHID AND r5.HdrRelRuleTID = 1000170
                INNER JOIN fbMasHdrRelation r6 ON r5.HdrRelParentHID = r6.HdrRelChildHID AND r6.HdrRelRuleTID = 1000169
                INNER JOIN fbMasHdr h1 ON r1.HdrRelParentHID = h1.HdrHID
                INNER JOIN fbMasHdr h2 ON r2.HdrRelParentHID = h2.HdrHID
                INNER JOIN fbMasHdr h3 ON r3.HdrRelParentHID = h3.HdrHID
                INNER JOIN fbMasHdr h4 ON r4.HdrRelParentHID = h4.HdrHID
                INNER JOIN fbMasHdr h5 ON r5.HdrRelParentHID = h5.HdrHID
                INNER JOIN fbMasHdr h6 ON r6.HdrRelParentHID = h6.HdrHID
                WHERE h6.HdrCode = 'SOLD'
                )

                SELECT count(*)
                                
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
                                LEFT JOIN CTE AS CTE ON CTE.HdrHID=PROP.HdrHID 
                                        WHERE  (hdr.TxnProdDate BETWEEN '1/1/2017' AND '12/31/2020') AND CTE.HdrHID IS  NULL  
                                        and corp.hdrhid={X}
                                
        """

corp_query="""

            with cte as 
            (
            SELECT wc.HdrHID
            FROM fbMasHdr wc
            INNER JOIN fbMasHdrRelation r1 ON wc.HdrHID = r1.HdrRelChildHID AND r1.HdrRelRuleTID = 1000238
            INNER JOIN fbMasHdrRelation r2 ON r1.HdrRelParentHID = r2.HdrRelChildHID AND r2.HdrRelRuleTID = 1000177
            INNER JOIN fbMasHdrRelation r3 ON r2.HdrRelParentHID = r3.HdrRelChildHID AND r3.HdrRelRuleTID = 1000176
            INNER JOIN fbMasHdrRelation r4 ON r3.HdrRelParentHID = r4.HdrRelChildHID AND r4.HdrRelRuleTID = 1000175
            INNER JOIN fbMasHdrRelation r5 ON r4.HdrRelParentHID = r5.HdrRelChildHID AND r5.HdrRelRuleTID = 1000170
            INNER JOIN fbMasHdrRelation r6 ON r5.HdrRelParentHID = r6.HdrRelChildHID AND r6.HdrRelRuleTID = 1000169
            INNER JOIN fbMasHdr h1 ON r1.HdrRelParentHID = h1.HdrHID
            INNER JOIN fbMasHdr h2 ON r2.HdrRelParentHID = h2.HdrHID
            INNER JOIN fbMasHdr h3 ON r3.HdrRelParentHID = h3.HdrHID
            INNER JOIN fbMasHdr h4 ON r4.HdrRelParentHID = h4.HdrHID
            INNER JOIN fbMasHdr h5 ON r5.HdrRelParentHID = h5.HdrHID
            INNER JOIN fbMasHdr h6 ON r6.HdrRelParentHID = h6.HdrHID
            WHERE h6.HdrCode = 'SOLD'
            )
            select  corp.hdrhid, count(*)
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
                            LEFT JOIN CTE AS CTE ON CTE.HdrHID=PROP.HdrHID 
            where CTE.HdrHID IS  NULL  
            group by corp.hdrhid

            """

query="""
                with cte as 
                        (
                        SELECT DISTINCT wc.HdrHID
                        FROM fbMasHdr wc
                        INNER JOIN fbMasHdrRelation r1 ON wc.HdrHID = r1.HdrRelChildHID AND r1.HdrRelRuleTID = 1000238
                        INNER JOIN fbMasHdrRelation r2 ON r1.HdrRelParentHID = r2.HdrRelChildHID AND r2.HdrRelRuleTID = 1000177
                        INNER JOIN fbMasHdrRelation r3 ON r2.HdrRelParentHID = r3.HdrRelChildHID AND r3.HdrRelRuleTID = 1000176
                        INNER JOIN fbMasHdrRelation r4 ON r3.HdrRelParentHID = r4.HdrRelChildHID AND r4.HdrRelRuleTID = 1000175
                        INNER JOIN fbMasHdrRelation r5 ON r4.HdrRelParentHID = r5.HdrRelChildHID AND r5.HdrRelRuleTID = 1000170
                        INNER JOIN fbMasHdrRelation r6 ON r5.HdrRelParentHID = r6.HdrRelChildHID AND r6.HdrRelRuleTID = 1000169
                        INNER JOIN fbMasHdr h1 ON r1.HdrRelParentHID = h1.HdrHID
                        INNER JOIN fbMasHdr h2 ON r2.HdrRelParentHID = h2.HdrHID
                        INNER JOIN fbMasHdr h3 ON r3.HdrRelParentHID = h3.HdrHID
                        INNER JOIN fbMasHdr h4 ON r4.HdrRelParentHID = h4.HdrHID
                        INNER JOIN fbMasHdr h5 ON r5.HdrRelParentHID = h5.HdrHID
                        INNER JOIN fbMasHdr h6 ON r6.HdrRelParentHID = h6.HdrHID
                        WHERE h6.HdrCode = 'SOLD'
                        )

                        SELECT rcorp.HdrCode AS BatchCorpCode, rcorp.HdrName AS BatchCorpName, corp.HdrCode AS GLCorpCode, corp.HdrName AS GLCorpName, acct.HdrCode AS AcctCode, acct.HdrName AS AcctName, icorp.HdrCode AS GLICCorpCode,
                  prop.HdrCode AS PropCode, prop.HdrName AS PropName, purch.HdrCode AS PurchCode, purch.HdrName AS PurchName,  gl.GlDtlTID, gl.DtlCorpHID, gl.DtlAcctHID, gl.DtlBatchTID, gl.DtlBatchNo, gl.DtlProcessTID,
                  gl.DtlBankTxnTID, gl.DtlChkDepNo, gl.DtlInvoiceNo, gl.DtlTxnType, gl.DtlTxnSrcCode, gl.DtlTxnDate, gl.DtlAcctDate, gl.DtlSvcDate, gl.DtlAfeCatCode, gl.DtlBillCatCode, gl.DtlAtrType, gl.DtlAtrCode, gl.DtlProdCode, gl.DtlProdCmpnt,
                  gl.DtlProdDsgnCode, gl.DtlUomCode, gl.DtlTaxStateCode, gl.DtlIntTypeCode, gl.DtlSysIntCode, gl.DtlVol, gl.DtlDesc, gl.DtlVal, gl.DtlVendorHID, gl.DtlPayeeHID, gl.DtlPurchaserHID, gl.DtlOwnerHID, gl.DtlRemitterHID, gl.DtlAfeHID,
                  gl.DtlPropHID, gl.DtlICCorpHID, gl.DtlFisPeriodClosingTID, gl.DtlDdaOwnerHID, gl.DtlDistrib, gl.DtlCurrTransHistSpecRate, gl.DtlEqDtlTID, gl.DtlTxnTypeUserDefCode,glTxn.OrigTxnRowTID
				 
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
				  LEFT JOIN CTE AS CTE ON CTE.HdrHID=PROP.HdrHID 
                        WHERE  (hdr.TxnProdDate BETWEEN '1/1/2017' AND '12/31/2020') AND CTE.HdrHID IS  NULL  
						
						and corp.hdrhid='{X}'
                        ORDER BY hdr.TxnProdDate
                        OFFSET {Y} ROWS
                        FETCH NEXT {Z} ROWS ONLY
                                """



import math    
import pandas as pd
import pyodbc
import numpy as np
import importlib.util


server="server"
db="database"
user="database_user"
pws="user_pass"

def controller():
    try: 
        ctx = pyodbc.connect('DRIVER={ODBC DRIVER 17 for SQL Server};SERVER='
                                +server+';DATABASE='+db+';UID='+user+';PWD='+pws)
        print('Successful connection')
    except: 

        print ('Connection Fails' )

    cs = ctx.cursor()
    print('cursor ok')
    return cs, ctx


def get_corporates(ctx, corp_query):
    print('Get Corporate list Started')
    corp=pd.read_sql(corp_query, ctx)
    corp_list=corp.iloc[:,0].tolist()
    print('Corporate list CREATED')
    return corp_list 


def get_count(ctx, c_query,corp,size):

    print('\n')
    print('starting count query')
    c_data=pd.read_sql(c_query.format(X=corp), ctx)
    print('C_query output = {}'.format(corp))
    iterator=math.ceil(int(c_data.iloc[0,0])/size)
    print('Iterator = {}'.format(iterator))
    
    # print('Starting Query')
    # data = pd.read_sql(query[0],ctx)
    # viewNm = viewNm
    # print('Data created')
    # print(data)
    return iterator





def get_splitData(query, corp, iterator,Y,Z):
    
    print('Split Data started')
    data=pd.read_sql(query.format(X=corp, Y=Y, Z=Z) ,ctx)
    print('Split data Finished')
    return data

    



def create_file(data,corp, viewNm, iterator):
    print('Creating file')
    # with pd.ExcelWriter("C:\\Users\\ca6359\\Desktop\\Hilcorp\\Output Files\\{}_10_V{}.xlsx".format(viewNm,iterator)) as writer:  
    #     data.to_excel(writer, sheet_name='Sheet_name_1')
    
    writer = pd.ExcelWriter("G:\\Accounting\\SAN JUAN\\PPA Documentation\\Data Extraction_IT3\\glMasDtl\\Extract_{}_CorpID_{}_File_{}.xlsx".format(viewNm,corp,iterator), engine='xlsxwriter')

    # Convert the dataframe to an XlsxWriter Excel object.
    data.to_excel(writer, sheet_name='Sheet1', index=False)

    # Close the Pandas Excel writer and output the Excel file.
    writer.save()
    print('File {} - V{} Created'.format(viewNm,iterator))
#_______________________________________________________Main_____________________________________


 

cs, ctx = controller() 
corp_list=get_corporates(ctx, corp_query)
# corp_list=['1070503']
size=400000
for corp in corp_list:
 
    # corp_id=corp_list[0]
    iterator = get_count(ctx, c_query, corp, size)  
    print('                                   Iterator number =',iterator, 'For the Corp {}'.format(corp))
    for cicle in range(iterator):
        print(cicle)
        Y=size*cicle
        Z=size   #*(cicle+1)
        print('The range in this cicle is   {}  to      {}  '.format(Y,Z))
        data = get_splitData(query, corp, iterator,Y,Z)
        create_file(data,corp, 'glMasDtl', cicle)
        # print(data)
        # break
    # break
