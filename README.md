# jaffle_shop_with_fal

It is year 2 for our [jaffle shop](https://github.com/dbt-labs/jaffle_shop) and the shop owner started collecting advanced attributes about the [orders](https://github.com/fal-ai/jaffle_shop_with_fal/blob/main/seeds/raw_order_attributes.csv).

We are tasked to understand what kind of jaffles we make the most money from.

So we decided to run a [clustering algorithm](https://github.com/fal-ai/jaffle_shop_with_fal/blob/main/clustering.py) to separate the orders into 3 different clusters and then to calculate all the [revenue for each cluster](https://github.com/fal-ai/jaffle_shop_with_fal/blob/main/models/cluster_stats.sql).

[Fal](https://github.com/fal-ai/fal) is the perfect tool for the task at hand, by using the [`fal flow`](https://blog.fal.ai/python-or-sql-why-not-both/) command, we can run our clustering python script in the middle of 2 dbt models. 


### Installing Instructions:

1) Install fal
```
$ pip install fal
```

2) Install KModes to run the clustering script.
```
$ pip install kmodes
```

3) Run dbt seed
```
$ dbt seed
```

4) Change the source with your own dataset name; https://github.com/fal-ai/jaffle_shop_with_fal/blob/main/models/schema.yml#L5

### Running Instructions:

Run fal flow 
```bash
$ fal flow run --experimental-flow
## runs the whole graph
```

Alternatively run fal flow with [graph selectors](https://docs.getdbt.com/reference/node-selection/graph-operators)
```bash
$ fal flow run --experimental-flow --select clustering.py+
## runs clustering.py and cluster_stats.sql
```

