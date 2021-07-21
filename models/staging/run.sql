with run as (
    '{{ env_var("DBT_CLOUD_PROJECT_ID", "manual") }}' as project_id,
    '{{ env_var("DBT_CLOUD_JOB_ID", "manual") }}' as job_id,
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' as run_id
)

select * from run