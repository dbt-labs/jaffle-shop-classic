## Testing dbt project: `jaffle_shop`

`jaffle_shop` is a fictional ecommerce store. This dbt project transforms raw data from an app database into customers and orders warehouse models and some basic analytics models.
### What is this repo?
What this repo _is_:
- A repo for aspiring dbt gatekeepers to put their learnings to the test and transform a seemingly unstructured repo into one fit for the Octopus datalake.   

What this repo _is not_:
- A tutorial — check out the [Getting Started Tutorial](https://docs.getdbt.com/tutorial/setting-up) for that. Notably, this repo contains some anti-patterns to make it self-contained, namely the use of seeds instead of sources.
- A demonstration of using dbt for a high-complex project, or a demo of advanced features — some of these are included and we'll add to them over time but for now we're just trying to keep things simple here!

### What's in this repo?
This repo contains [seeds](https://docs.getdbt.com/docs/building-a-dbt-project/seeds) that includes some (fake) raw data from a fictional app.

The raw data consists of customers, orders, and payments, with the following entity-relationship diagram:

![Jaffle Shop ERD](/etc/jaffle_shop_erd.png)

### How to use this repo and become a gatekeeper?

In its base state the repo is not fit for purpose. While it works, it doesn't comply with many of the conventions we enforce at octopus so your goal is to remedy that.

Use `make init` to get started with running the rest of the make commands. 

### So what needs doing to the repo?

The point of being a gatekeeper is being able to look at a PR and know where to look for possible convention breaches.
Check the [data platform docs](http://docs.eks.octopus.engineering/reference/dbt_gatekeeper_checklist/) site for tips on how to gatekeep.  

#### Fixes

Here are the fixes that need implementing:

1) All `.yml` files should be renamed to specify what they apply to. For example each model directory should contain a `_models.yml` file (the `_` is to ensure the file is top of the directory for easy access) and may or may not contain a `_docs.yml` file for documentation. 
2) Staging models should be split by which source they are coming from. As the sources in this repo all come from seeds, the staging models on top of them should be in the `src_seed` directory along with their respective `_models.yml` and `_sources.yml` files.
3) stg_customers contains PII data in the `first_name` and `last_name` columns so these need to be hashed. Mark the model and each of the sensitive columns as sensitive in the `src_seed/_models.yml` using the syntax:
    ```
    models:
      - name: stg_customers_pii
        meta:
          owner: 'example.email@octoenergy.com'
          sensitive: true
        description: |
          Table description
        columns:
          - name: customer_id
            tests:
              - unique
              - not_null
          - name: first_name
            meta:
              sensitive: true
          - name: last_name
            meta:
              sensitive: true
    ```
   You can refer to [dbt Project Architecture](https://docs.eks.octopus.engineering/explanations/dbt_project_architecture/#PII) doc for further information on handling PII.
4) The `customers.sql` and `orders.sql` models are traditional warehouse models and should be in a `warehouse` directory with their respective `_docs.md` and `_models.yml` files.
5) We use a package to test the structure of the dbt project called [dbt_project_evaluator](https://github.com/dbt-labs/dbt-project-evaluator) - this tests for lineage issues. One of its major checks is to see if staging models refer to other staging models which is normally not allowed. 
 
   However, we need to do this when hashing sensitive models so we need to make an exception. To do this, create a new seed called `dbt_project_evaluator_exceptions.csv` with the following content:
   ```
   fct_name,column_name,id_to_exclude,comment
   fct_staging_dependent_on_staging,parent,stg_customers_pii,Scrubbing pii permitted in staging layer.
   ```
   This will disable the `fct_staging_dependent_on_staging` test for the `stg_customers_pii` where it is the parent of another staging model and give a reason for why its been omitted: `Scrubbing pii permitted in staging layer.`

   This is a bit niche but dbt_project_evaluator will become a big part of our testing process in future so its important to have an understanding of how it works. 
#### New Models

You've also had a request from the SMT asking for two dashboards, one for finance and one for sales. They need the following shown:
- Finance - Total value of orders returned by customer
- Sales - The customer count by month for customers making their first order

There are two possible approaches to this:
1) -  How we do things at the time of writing - Create a final model per dashboard showing the relevant information and assign an exposure to each with a dummy URL :
     ```
     url: https://inksacio.eks.octopus.engineering/my_certification_dashboard/
     ```
   - Put each model into a directory specific to their business unit like `models/final/sales/fnl_sales_newcustomers.sql`
   - Make sure to write a `_models.yml` in each directory.
2) How we will do things in future - Make the required data available via [metrics](https://docs.getdbt.com/docs/build/metrics) configured directly on the warehouse model configs or in a `_metrics.yml` file. 

   For example:
   ```
   metrics:
   - name: new_customers
     label: New Customers
     model: ref('wh_customers')
     description: ""

     calculation_method: count_distinct
     expression: customer_id

     timestamp: first_order
     time_grains: [day, week, month, quarter, year]

     # general properties
     config:
       enabled: true 

     meta: {team: Sales}
   ```








You can use `make run-python-tests` command to see if your changes have worked or alternatively when you make a PR from your branch CircleCI will run tests to ensure that your changes comply with Octopus conventions. This will run the first set of tests.

If all your tests pass... You've passed this section of the certification! Let one of the @dbt_gatekeepers know and send them a link to your PR.
Remember not to merge it, the repo is broken on purpose! 



### What is a jaffle?
A jaffle is a toasted sandwich with crimped, sealed edges. Invented in Bondi in 1949, the humble jaffle is an Australian classic. The sealed edges allow jaffle-eaters to enjoy liquid fillings inside the sandwich, which reach temperatures close to the core of the earth during cooking. Often consumed at home after a night out, the most classic filling is tinned spaghetti, while my personal favourite is leftover beef stew with melted cheese.

---
For more information on dbt:
- Read the [introduction to dbt](https://docs.getdbt.com/docs/introduction).
- Read the [dbt viewpoint](https://docs.getdbt.com/docs/about/viewpoint).
- Join the [dbt community](http://community.getdbt.com/).
---
