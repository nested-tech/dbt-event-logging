{% macro get_audit_relation() %}
    
    {% if target.name == 'prod' %}
    {% set schema = target.schema~'_meta' %}
    {% else %}
    {% set schema = target.schema %}
    {% endif %}

    {%- set audit_table = 
        api.Relation.create(
            identifier='dbt_audit_log', 
            schema=schema, 
            type='table'
        ) -%}
    {{ return(audit_table) }}
{% endmacro %}


{% macro get_audit_schema() %}
    {% set audit_table = logging.get_audit_relation() %}
    {{ return(audit_table.include(schema=True, identifier=False)) }}    
{% endmacro %}


{% macro log_audit_event(event_name, schema, relation) %}

    insert into {{ logging.get_audit_relation() }} (
        event_name, 
        event_timestamp, 
        event_schema, 
        event_model,
        invocation_id
        ) 
    
    values (
        '{{ event_name }}', 
        {{dbt_utils.current_timestamp_in_utc()}}, 
        {% if variable != None %}'{{ schema }}'{% else %}{{ dbt_utils.safe_cast('null', dbt_utils.type_string()) }}{% endif %}, 
        {% if variable != None %}'{{ relation }}'{% else %}{{ dbt_utils.safe_cast('null', dbt_utils.type_string()) }}{% endif %}, 
        '{{ invocation_id }}'
        )

{% endmacro %}


{% macro create_audit_log_table() %}

    create table if not exists {{ logging.get_audit_relation() }}
    (
       event_name       {{dbt_utils.type_string()}},
       event_timestamp  {{dbt_utils.type_timestamp()}},
       event_schema     {{dbt_utils.type_string()}},
       event_model      {{dbt_utils.type_string()}},
       invocation_id    {{dbt_utils.type_string()}}
    )

{% endmacro %}


{% macro log_run_start_event() %}
    {{logging.log_audit_event('run started')}}
{% endmacro %}


{% macro log_run_end_event() %}
    {{logging.log_audit_event('run completed')}}
{% endmacro %}


{% macro log_model_start_event() %}
    {{logging.log_audit_event(
        'model deployment started', this.schema, this.name
        )}}
{% endmacro %}


{% macro log_model_end_event() %}
    {{logging.log_audit_event(
        'model deployment completed', this.schema, this.name
        )}}
{% endmacro %}