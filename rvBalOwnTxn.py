c_query="""
          select 
                COUNT(*)
                FROM rvBalOwnTxn bal
                INNER JOIN rvTxnDtl dtl ON bal.OwnTxnDtlTID = dtl.TxnDtlTID
                INNER JOIN rvTxnHdr hdr ON dtl.TxnHdrTID = hdr.TxnHdrTID
                INNER JOIN rvTxnChkHdr chk ON hdr.TxnChkTID = chk.TxnChkTID
                INNER JOIN aaMasBatch b ON chk.TxnBatchTID = b.BatchTID
                INNER JOIN aaTblIntType tpe ON bal.OwnIntCode = tpe.IntTypeCode
                INNER JOIN fbMasHdr own ON bal.OwnHID = own.HdrHID
                INNER JOIN fbMasHdr corp ON bal.OwnCorpHID = corp.HdrHID
                INNER JOIN aaMasAddress addr ON bal.OwnAddrTID = addr.AddrTID
                INNER JOIN aaMasOwn oattr ON bal.OwnHID = oattr.OwnHID
                INNER JOIN fbMasHdr prop ON bal.OwnPropHID = prop.HdrHID
                INNER JOIN fbMasHdr purch ON chk.TxnPurchHID = purch.HdrHID
                left join ##temp_soldwells AS sw ON sw.HdrHID=PROP.HdrHID 
                WHERE bal.OwnPmtStatCode NOT IN ('Xfer')
                AND b.BatchPosted = 1
                and bal.OwnProdDate between '2017-01-01 00:00:00' and '2017-01-31 00:00:00' 
                and sw.HdrHID is null
                AND DTL.TxnReversed =0 
                AND dtl.TxnReversal=0 
                     
                                
        """

corp_query="""

            

            """

query="""
             select 
                 OwnTxnTID
                ,TRIM(prop.HdrCode) as PropCode
                ,TRIM(purch.HdrCode) as PurchCode
                ,TRIM(TxnProdCode) AS TxnProdCode
                ,OwnProdDate
                ,TRIM(own.HdrCode) as OwnCode
                ,TRIM(SysIntCode) AS SysIntCode
                ,TRIM(OwnIntCode) AS OwnIntCode
                ,cast (OwnDeckDcml as varchar) OwnDeckDcml
                ,OwnChkVol
                ,OwnMmbtu
                ,OwnVal
                ,OwnNet
                FROM rvBalOwnTxn bal                
                INNER JOIN rvTxnDtl dtl ON bal.OwnTxnDtlTID = dtl.TxnDtlTID
                INNER JOIN rvTxnHdr hdr ON dtl.TxnHdrTID = hdr.TxnHdrTID
                INNER JOIN rvTxnChkHdr chk ON hdr.TxnChkTID = chk.TxnChkTID
                INNER JOIN aaMasBatch b ON chk.TxnBatchTID = b.BatchTID
                INNER JOIN aaTblIntType tpe ON bal.OwnIntCode = tpe.IntTypeCode
                INNER JOIN fbMasHdr own ON bal.OwnHID = own.HdrHID
                INNER JOIN fbMasHdr corp ON bal.OwnCorpHID = corp.HdrHID
                INNER JOIN aaMasAddress addr ON bal.OwnAddrTID = addr.AddrTID
                INNER JOIN aaMasOwn oattr ON bal.OwnHID = oattr.OwnHID
                INNER JOIN fbMasHdr prop ON bal.OwnPropHID = prop.HdrHID
                INNER JOIN fbMasHdr purch ON chk.TxnPurchHID = purch.HdrHID
                left join ##temp_soldwells AS sw ON sw.HdrHID=PROP.HdrHID 
                WHERE bal.OwnPmtStatCode NOT IN ('Xfer')
                AND b.BatchPosted = 1
                and bal.OwnProdDate between '2017-01-01 00:00:00' and '2017-01-31 00:00:00' 
                and sw.HdrHID is null
                AND DTL.TxnReversed =0 
                AND dtl.TxnReversal=0 
                ORDER BY bal.OwnProdDate
                OFFSET {Y} ROWS
                FETCH NEXT {Z} ROWS ONLY
                                """



import math    
import pandas as pd
import pyodbc
import numpy as np
import importlib.util


server="enertiagemini"
db="Enertia_Gemini_M3"
user="GeminiData_User"
pws="Gem1n!_us#r"

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


def get_count(ctx, c_query,corp, size):

    print('\n')
    print('starting count query')
    c_data=pd.read_sql(c_query, ctx)
    print('C_query output = ')
    iterator=math.ceil(int(c_data.iloc[0,0])/size)
    print('Iterator = {}'.format(iterator))
    
  
    return iterator





def get_splitData(query, corp, iterator,Y,Z):
    
    print('Split Data started')
    data=pd.read_sql(query.format(X=corp, Y=Y, Z=Z) ,ctx)
    print('Split data Finished')
    return data

    

def create_file(data,corp, viewNm, iterator):
    print('Creating file')
    writer = pd.ExcelWriter("G:\\Accounting\\SAN JUAN\\PPA Documentation\\Data Extraction_IT3\\{}\\Extract_{}_CorpID_{}_File_{}.xlsx".format(viewNm, viewNm,corp,iterator), engine='xlsxwriter')

    # Convert the dataframe to an XlsxWriter Excel object.
    data.to_excel(writer, sheet_name='Sheet1', index=False)

    # Close the Pandas Excel writer and output the Excel file.
    writer.save()
    print('File {} - V{} Created'.format(viewNm,iterator))


def create_CSVfile(data,corp, viewNm, iterator):
    print('Creating Flat File ')
    data.to_csv( "G:\\Accounting\\SAN JUAN\\PPA Documentation\\Data Extraction_IT3\\{}\\Extract_{}_File_{}.txt".format(viewNm,viewNm,iterator)
                ,sep='|' , index=False   )
    print('File {}  Created...'.format(iterator))


def create_FtFile (data,corp, viewNm, iterator):
    print('Creating Flat File ')
    np.savetxt("G:\\Accounting\\SAN JUAN\\PPA Documentation\\Data Extraction_IT3\\{}\\Extract_{}_File_{}.csv".format(viewNm,viewNm,iterator)
            , data.values, fmt='%d',delimiter='|')
    print('File {}  Created...'.format(iterator))

#_______________________________________________________Main_____________________________________


 

cs, ctx = controller() 
# corp_list=get_corporates(ctx, corp_query)
# corp_list=['1070503']
size=400000
# for corp in corp_list:
 
# corp_id=corp_list[0]
iterator = get_count(ctx, c_query, 'corp', size)  
print('                                   Iterator number =',iterator, 'For the Corp {}'.format('ALL'))
for cicle in range(iterator):
    print(cicle)
    Y=size*cicle
    Z=size   #*(cicle+1)
    print('The range in this cicle is   {}  to      {}  '.format(Y,Z))
    data = get_splitData(query, 'corp', iterator,Y,Z)
    create_CSVfile(data,'corp', 'rvBalOwnTxn', cicle)
    # print(data)
    # break
# break
