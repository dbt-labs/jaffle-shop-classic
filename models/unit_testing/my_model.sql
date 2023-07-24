SELECT a+b as c, concat(string_a, string_b) as string_c, not_testing, date_a, 
{{ dbt.string_literal(type_numeric()) }} as macro_call,
{{ dbt.string_literal(var('test', 'default')) }} as var_call,
{{ dbt.string_literal(env_var('TEST', 'default')) }} as env_var_call,
{{ dbt.string_literal(invocation_id) }} as invocation_id
FROM {{ ref('my_model_a')}} my_model_a
JOIN {{ ref('my_model_b' )}} my_model_b
ON my_model_a.id = my_model_b.id