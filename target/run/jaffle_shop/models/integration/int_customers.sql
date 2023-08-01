
      -- back compat for old kwarg name
  
  
        
            
            
        
    

    

    merge into "dbtsales"."public"."int_customers" as DBT_INTERNAL_DEST
        using "int_customers__dbt_tmp080303552078" as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.customer_name = DBT_INTERNAL_DEST.customer_name
            )

    
    when matched then update set
        "customer_id" = DBT_INTERNAL_SOURCE."customer_id","customer_name" = DBT_INTERNAL_SOURCE."customer_name","dtloaded" = DBT_INTERNAL_SOURCE."dtloaded","dtupdated" = DBT_INTERNAL_SOURCE."dtupdated","dtinserted" = DBT_INTERNAL_SOURCE."dtinserted"
    

    when not matched then insert
        ("customer_id", "customer_name", "dtloaded", "dtupdated", "dtinserted")
    values
        ("customer_id", "customer_name", "dtloaded", "dtupdated", "dtinserted")


  