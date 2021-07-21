with env_var as (
    '{{ env_var("DBT_CLOUD_PROJECT_ID") }}' as project_id,
    '{{ env_var("DBT_CLOUD_JOB_ID") }}' as job_id,
    '{{ env_var("DBT_CLOUD_RUN_ID") }}' as run_id
)

select * from env_var