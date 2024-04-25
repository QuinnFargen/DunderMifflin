
from faker import Faker
import pymssql
import pyodbc
from datetime import  date, timedelta
import random

def dailysales():       
    fake = Faker()
    conn = get_sqlconn()
    Dt = date.today().strftime('%Y-%m-%d')
    numSales = get_numSales(date.today())

    for _ in range(numSales):
        get_fakeSale(fake, conn, Dt)

    end_sqlconn(conn)
    
def catchupsales(start_dt, end_dt = None):
    if end_dt is None:
        end_dt = date.today()       
    fake = Faker()
    conn = get_sqlconn()

    for single_date in daterange(start_dt, end_dt):

        Dt = single_date.strftime('%Y-%m-%d')
        numSales = get_numSales(single_date)

        for _ in range(numSales):
            get_fakeSale(fake, conn, Dt)
        
        dailytableProc()      
            
    end_sqlconn(conn)
 
def get_fakeSale(fake, conn, Dt = None):
    Cust = fake.json(data_columns={'Customer':{'Name':'name', 'Address':'address', 'Company':'company', 'Title':'job', 'Phone':'phone_number'}}, num_rows=1)
    Order = fake.json(data_columns=[('ProductID','random_int',{'min':1, 'max':78}),('Quantity','random_int',{'min':1, 'max':25})], num_rows=fake.random_int(min=1, max=10))
    cursor = conn.cursor()

    # pymssql   -- Azure SQL
    sql = "INSERT INTO [staging].[FakeSales] ([Customer], [Order], [SaleDate]) VALUES (%s, %s, %s)"
    cursor.execute(sql, (Cust, Order, Dt)) 
        
    # # pyodbc -- Local MSSQL
    # sql = "INSERT INTO [staging].[FakeSales] ([Customer], [Order], [SaleDate]) VALUES (?, ?, ?)"
    # cursor.execute(sql, [Cust, Order, Dt]) 

    conn.commit()

def get_sqlconn():    
    # conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=DESKTOP-#######\SQLEXPRESS;DATABASE=DunderMifflin;UID=michaelscott;PWD=[Av3ry$ecur3PW]') 
    conn = pymssql.connect(server='[Server Name].database.windows.net', user='michaelscott', password='[Av3ry$ecur3PW]', database='DunderMifflin')
    return conn

def end_sqlconn(conn):   
    conn.close()

    
def dailytableProc():
    connsp = get_sqlconn()
    cursorsp = connsp.cursor()  
    cursorsp.execute('exec Dundermifflin.staging.dm_add_fakesale')   
    connsp.commit() 
    end_sqlconn(connsp)  

def daterange(start_dt, end_dt):
    for n in range(int((end_dt - start_dt).days)):
        yield start_dt + timedelta(n)

def get_numSales(Dt):  
    yr = Dt.year - 2000
    # Should roughly increase sales every year
    # Company founded 2009, so assumption that year is after 2000
    numSales = random.randint(yr-5,yr+5)
    return numSales


start_dt = date(2009, 4, 20)
end_dt = date(2024, 4, 20)
catchupsales(start_dt,end_dt)

dt = date(2024, 4, 20)
catchupsales(dt)