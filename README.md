# DunderMifflin

This is a fork of [Tim Mitchell's DunderMifflin](https://github.com/tdmitch/DunderMifflin) database.
Here is a [dbdiagram](https://dbdiagram.io/d/DunderMifflin-6621747a03593b6b615e5404) of how he has it initially setup.<br>
Tim's database is a static demo database with amazing detail to the original show.

# New Paper Company

[TheMichaelScottPaperCompany.org](https://themichaelscottpapercompany.org)<br>
I plan to modify the structure slightly and to add daily sales to the database to bring it to life.<br>
With the use of Airflow & Faker, I will add made-up orders to the database daily.<br>
These sales will be monitored with a Metabase dashboard hosted at url above.

## Database Design

Here is my [dbdiagram](https://dbdiagram.io/d/DunderMifflin-Quinns-66217df403593b6b615ef515) modifications from Tim's initial database.
The main change is a [new business model](https://youtu.be/r-GFmH0EK9Y?si=BZb3Tn5cemq1GEND&t=236) of DTC, this fit better the new/fake daily sales plan.

- Added Azure external data source to import source data from Azure storage account.
- Added Inventory dimension and moved columns from Products to new table.
- Added Affiliates dimension to Customers and removed CustomerCode column. 
- Archived Customer, Order & OrderDetail source data, will generate all with fake data.
- Removed the EmployeeRegions dimension and added RegionID directly to Employees table.
- Renamed Orders column ShipVia to ShipperID. Renamed Employees, Customers, Orders columns Region to ST.

## Bringing to Life

Using [Google Composer](https://cloud.google.com/composer?hl=en) and [Apache Airflow](https://airflow.apache.org) for orchestration, a python script will add fake sales.<br>
The fake sales will be generated within the script with the python package [Faker](https://faker.readthedocs.io/en/master/).<br>
Everyday the DAG file will insert fake sales and then execute a stored procedure to process the records into the tables.<br>

## Dashboard Monitoring

[Docker](https://www.docker.com/products/docker-desktop/) was used to host [Metabase](https://www.metabase.com) and [Nginx](https://hub.docker.com/r/jc21/nginx-proxy-manager) for developing reporting on [TheMichaelScottPaperCompany.org](https://themichaelscottpapercompany.org)<br> 
Please reach out to me if guest access is wanted to metabase or the active database.

