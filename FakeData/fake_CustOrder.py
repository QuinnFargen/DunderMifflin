
from faker import Faker
import pymssql
from datetime import  date, timedelta
import random

def dailysales():
    numSales = random.randint(15,25)        
    fake = Faker()
    conn = get_sqlconn()

    for _ in range(numSales):
        get_fakeSale(fake, conn)

    end_sqlconn(conn)
    
def catchupsales(start_dt, end_dt = None):
    if end_dt is None:
        end_dt = date.today()       
    fake = Faker()
    conn = get_sqlconn()

    for single_date in daterange(start_dt, end_dt):

        Dt = single_date.strftime('%Y-%m-%d')
        numSales = random.randint(15,25)

        for _ in range(numSales):
            get_fakeSale(fake, conn, Dt)
            
    end_sqlconn(conn)

def get_fakeSale(fake, conn, Dt):
    Cust = fake.json(data_columns={'Customer':{'Name':'name', 'Address':'address', 'Title':'job', 'Phone':'phone_number'}}, num_rows=1)
    Order = fake.json(data_columns=[('ProductID','random_int',{'min':1, 'max':78}),('Quantity','random_int',{'min':1, 'max':100})], num_rows=fake.random_int(min=1, max=20))

    cursor = conn.cursor()
    if Dt is None:
        sql = "INSERT INTO [staging].[FakeSales] ([Customer], [Order]) VALUES (%s, %s)"
        cursor.execute(sql, (Cust, Order))
    else:
        sql = "INSERT INTO [staging].[FakeSales] ([Customer], [Order], [SaleDate]) VALUES (%s, %s, %s)"
        cursor.execute(sql, (Cust, Order, Dt))

    conn.commit()

def get_sqlconn():    
    conn = pymssql.connect(server='[Server Name].database.windows.net', user='michaelscott', password='[Av3ry$ecur3PW]', database='DunderMifflin')
    return conn

def end_sqlconn(conn):   
    conn.close()

def daterange(start_dt, end_dt):
    for n in range(int((end_dt - start_dt).days)):
        yield start_dt + timedelta(n)


# start_dt = date(2024, 4, 15)
# end_dt = date(2024, 4, 20)
# catchupsales(start_dt)
