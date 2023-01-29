"""
This code is used to pull 200 millions of records from a database to a size delimited flat files.
The purpose of this code is migrate Enertia ERP system Data to SAP.

"""

c_query="""
          select 
                COUNT(*)
                FROM tempdb..camilo_query
                WHERE OwnProdDate between '2017-01-01 00:00:00' and '2017-12-31 00:00:00'   
                
                     
                                
        """

corp_query="""

            

            """

query="""
             select
                 OwnTxnTID
                , PropCode
                ,PurchCode
                ,TxnProdCode
                ,OwnProdDate
                ,OwnCode
                ,SysIntCode
                , OwnIntCode
                ,OwnDeckDcml
                ,OwnChkVol
                ,OwnMmbtu
                ,OwnVal
                ,OwnNet 

			
				FROM tempdb..camilo_query
                WHERE OwnProdDate between '2017-01-01 00:00:00' and '2017-12-31 00:00:00'   
                
                ORDER BY OwnProdDate
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
    writer = pd.ExcelWriter("c\\{}\\Extract_{}_CorpID_{}_File_{}.xlsx".format(viewNm, viewNm,corp,iterator), engine='xlsxwriter')

    # Convert the dataframe to an XlsxWriter Excel object.
    data.to_excel(writer, sheet_name='Sheet1', index=False)

    # Close the Pandas Excel writer and output the Excel file.
    writer.save()
    print('File {} - V{} Created'.format(viewNm,iterator))


def create_CSVfile(data,corp, viewNm, iterator):
    print('Creating Flat File ')
    data.to_csv( "G:\\Accounting\\SAN JUAN\\PPA Documentation\\Data Extraction_IT3\\{}\\Extract_{}_File_{}.txt".format(viewNm,viewNm,'{0:04}'.format(iterator))
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
    if cicle not in range(116):
        print(cicle)
        Y=size*cicle
        Z=size   #*(cicle+1)
        print('The range in this cicle is   {}  to      {}  '.format(Y,Z))
        data = get_splitData(query, 'corp', iterator,Y,Z)
        create_CSVfile(data,'corp', 'rvBalOwnTxn', cicle)
        # print(data)
        # break
    # break
