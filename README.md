# DunderMifflin

This is a fork of [Tim Mitchell's DunderMifflin](https://github.com/tdmitch/DunderMifflin) database.
Here is a [dbdiagram](https://dbdiagram.io/d/DunderMifflin-6621747a03593b6b615e5404) of how he has it initially setup.

# Modifications & Goals

Tim's database is a static demo database with amazing detail to the original show.
I plan to modify the structure slightly and to add daily sales to the database to bring it to life.
With the use of python Faker package & Airflow, I will add madeup orders to the database daily.
These sales will be monitored with a Metabase dashboard.

## Database Design

Here is my [dbdiagram](https://dbdiagram.io/d/DunderMifflin-Quinns-66217df403593b6b615ef515) modifications from Tim's initial database.

- Added Azure external data source to import source data from Azure storage account.
- Removed the EmployeeRegions dimension and added RegionID directly to Employees table.
- Renamed Orders column ShipVia to ShipperID.
- Renamed Employees, Customers, Orders columns Region to ST.

## Bringing to Life

Using [Google Composer](https://cloud.google.com/composer?hl=en) and Apache Airflow for orchestration, a python script daily will add fake sales to the database.
The fake sales will be generated within the script with the python package [Faker](https://faker.readthedocs.io/en/master/).
The daily DAG file will insert fake sales and then execute a stored procedure to process the records into the tables.

## Dashboard Monitoring

Docker was used to host Metabase for developing [reporting](https://themichaelscottpapercompany.org/public/dashboard/84a122b6-81bb-4c23-af6e-912a2ef068fb).  [Nginx](https://www.nginx.com) was used for reverse proxying metabase to an obligatory domain purchase of [TheMichaelScottPaperCompany.org](https://themichaelscottpapercompany.org).
(yes... the .com was already taken)  Please reach out to me if guest access is wanted to metabase or the active database.

