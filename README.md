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
0
![Jaffle Shop ERD](/etc/jaffle_shop_erd.png)

### How to use this repo and become a gatekeeper?

In its base state the repo is not fit for purpose. While it works, it doesn't comply with many of the conventions we enforce at octopus so your goal is to remedy that. 

When you make a PR from your branch CircleCI will run tests to ensure that your changes comply with Octopus conventions.
If all your tests pass... Congrats, You're a gatekeeper! Let one of the @dbt_gatekeepers know and send them a link to your PR.
Remember not to merge it, the repo is broken on purpose! 

### So what needs doing to the repo?

The point of being a gatekeeper is being able to look at a PR and know where to look for possible convention breaches. Check the [data platform docs](http://docs.eks.octopus.engineering/reference/dbt_gatekeeper_checklist/) site for tips on how to gatekeep.  

### What is a jaffle?
A jaffle is a toasted sandwich with crimped, sealed edges. Invented in Bondi in 1949, the humble jaffle is an Australian classic. The sealed edges allow jaffle-eaters to enjoy liquid fillings inside the sandwich, which reach temperatures close to the core of the earth during cooking. Often consumed at home after a night out, the most classic filling is tinned spaghetti, while my personal favourite is leftover beef stew with melted cheese.

---
For more information on dbt:
- Read the [introduction to dbt](https://docs.getdbt.com/docs/introduction).
- Read the [dbt viewpoint](https://docs.getdbt.com/docs/about/viewpoint).
- Join the [dbt community](http://community.getdbt.com/).
---
