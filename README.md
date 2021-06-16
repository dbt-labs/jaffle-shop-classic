# Analytical Storage - The jaffle shop

`jaffle_shop` is a fictional ecommerce store. This project transforms raw data from an app database into a customers and orders model ready for analytics.
<details>
<summary>
<strong>What is a jaffle?</strong>
</summary>
A jaffle is a toasted sandwich with crimped, sealed edges. Invented in Bondi in 1949, the humble jaffle is an Australian classic. The sealed edges allow jaffle-eaters to enjoy liquid fillings inside the sandwich, which reach temperatures close to the core of the earth during cooking. Often consumed at home after a night out, the most classic filling is tinned spaghetti, while my personal favourite is leftover beef stew with melted cheese.
</details>

The raw data consists of customers, orders, and payments, with the following entity-relationship diagram:

![Jaffle Shop ERD](/etc/jaffle_shop_erd.png)

## Analytics Engineering 🧑‍🍳

"Analytics teams have a workflow problem today. Too often, analysts operate in isolation, and this creates suboptimal outcomes. Knowledge is siloed. We too often rewrite analyses that a colleague had already written. We fail to grasp the nuances of datasets that we’re less familiar with. We differ in our calculations of a shared metric. As a result, organizations suffer from reduced decision speed and reduced decision quality.

Analytics doesn’t have to be this way. In fact, the playbook for solving these problems already exists — on our software engineering teams.

The same techniques that software engineering teams use to collaborate on the rapid creation of quality applications can apply to analytics. We believe it’s time to build an open set of tools and processes to make that happen." 

"The analytics engineer sits at the intersection of the skill sets of data scientists, analysts, and data engineers. They bring a formal and rigorous software engineering practice to the efforts of analysts and data scientists, and they bring an analytical and business-outcomes mindset to the efforts of data engineering. It’s their job to build tools and infrastructure to support the efforts of the analytics and data team as a whole."

In our course we will learn the basic powers of analytics engineers.

## Data Transformation 🍳 

Is the work we put into structuring, enriching and converting the raw data to design new and meaningful data sets that allow us to perform analysis on top.

We use the power of cloud analytics storage to process the raw data we extracted and loaded. This is very compute-intensive, but that activity occurs in a highly powerful and scalable environment

We use AWS Redshift, which is a columnar databases, so index and record location operations are vastly quicker. And they’re also massively parallel databases, so the required transformations are carried out in parallel, not sequentially, with multiple nodes handling multiple transformations at the same time.

## SQL 🔪

SQL (Structured Query Language) is a programming language designed for managing data in a relational database. It's been around since the 1970s and is the most common method of accessing data in databases today. SQL has a variety of functions that allow its users to read, manipulate, and change data. Though SQL is commonly used by engineers in software development, it's also popular with data analysts for a few reasons:

- It's semantically easy to understand and learn.
- Because it can be used to access large amounts of data directly where it's stored, analysts don't have to copy data into other applications.
- Compared to spreadsheet tools, data analysis done in SQL is easy to audit and replicate. For analysts, this means no more looking for the cell with the typo in the formula.

SQL is great for performing the types of aggregations that you might normally do in an Excel pivot table—sums, counts, minimums and maximums, etc.—but over much larger datasets and on multiple tables at the same time.

## Assignment

All infos are included in the Pull Request template on the #Feedback branch!

### Additional Info
- [What is an Analytics Engineer](https://locallyoptimistic.com/post/analytics-engineer/)
- [Analytics Workflow](https://blog.fishtownanalytics.com/building-a-mature-analytics-workflow/)
- [S.Q.L or SEQUEL](https://medium.com/tableplus/how-to-pronounce-sql-properly-s-q-l-or-sequel-7203a5185676)
- [Redshift SQL commands](https://docs.aws.amazon.com/redshift/latest/dg/c_SQL_commands.html)