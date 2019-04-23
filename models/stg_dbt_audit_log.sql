{% set relation =  get_audit_relation() %}

with audit as (

    select * from `{{target.project}}`.{{relation.schema}}.dbt_audit_log

),

with_id as (

    select 
    
        *,
    
        {{dbt_utils.surrogate_key(
            'event_name', 
            'event_model', 
            'invocation_id'
            )}} as event_id
    
    from audit

)

select * from with_id