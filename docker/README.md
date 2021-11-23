# How to create a Jaffle Shop database

This directory contains files to start a database locally with all the data from the jaffle shop project.
This is helpful for testing, or just playing with the data contained in this dbt project.

**How it works**

Docker compose is used to run multiple related services together. In this case, we're running two services:

1. `postgres`: this is our database, we can connect to it to query the data using sql
2. `dbt`: this is a service that runs [dbt-core](https://github.com/dbt-labs/dbt-core) and converts this jaffle-shop project into useful data in postgres


## Step 1. Get the code

Open up your favourite terminal and clone this repo

```shell
git clone git@github.com:lightdash/jaffle-shop
cd jaffle-shop/docker
```

## Step 2. Get docker

You need to [install docker](https://docs.docker.com/get-docker/)

## Step 3. Create the database and fill it with jaffle data

Run the script:

```shell
./start.sh
```

This will start a postgres database and then run dbt to fill it with Jaffle Shop data!

The first time you run this command it might take a long time to download the docker images needed.

## Step 4. Check it worked

You can manually connect to the postgres database to check it has the data in

```shell
./connect.sh
```

You'll now be in a postgres console, try the following sql command:

```sql
select * from jaffle.payments;
```

If you don't see any data, something went wrong

## Step 5. Connect from another tool (like Lightdash!!)

You can use the following info to connect any tools to your postgres database:

```yaml
port: 5432
host: localhost
user: postgres
database: postgres
password: password
ssl: false
```

## Step 6. Stop all services

Battery running low on your laptop? Stop all the services and stop wasting power

```shell
./stop.sh
```

