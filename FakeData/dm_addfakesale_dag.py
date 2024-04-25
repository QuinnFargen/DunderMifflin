
from datetime import datetime, timedelta, date
import random
from airflow import DAG
from airflow.operators.python import PythonOperator

from faker import Faker
import pymssql
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

    
def dailysales_proc():
    connsp = get_sqlconn()
    cursorsp = connsp.cursor()  
    cursorsp.execute('exec Dundermifflin.staging.dm_add_fakesale')   
    connsp.commit() 
    end_sqlconn(connsp)  

def get_numSales(Dt):  
    yr = Dt.year - 2000
    # Should roughly increase sales every year
    # Company founded 2009, so assumption that year is after 2000
    numSales = random.randint(yr-5,yr+5)
    return numSales


with DAG(
    "FakeSales",
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="A DAG to create fake sales and insert into MSSQL DB.",
    start_date=datetime(2024, 4, 19),
    schedule_interval='0 14 * * *',
    # https://crontab.guru/#0_14_*_*_*
    # schedule=timedelta(days=1),
    catchup=False,
    tags=["dundermifflin"],
) as dag:

    t1 = PythonOperator(
        task_id="fake_dailysales_insert",
        python_callable=dailysales
    )
    t2 = PythonOperator(
        task_id="fake_dailysales_process",
        python_callable=dailysales_proc
    )

    t1 >> t2