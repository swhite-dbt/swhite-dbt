```yml
# dbt_project.yml
...
on-run-start: "{{ make_backups() }}"
on-run-end: "{{ check_results_drop_backups(results) }}"
```
```
make_backups.sql
-- macros/make_backups.sql
{% macro make_ts() %}
    {% set now = run_started_at.strftime("%Y%m%d_%H%M%S") %}
    {% do return(now) %}
{% endmacro %}

{% macro make_backups() %}
    {% if execute and target.name != 'prod' %}

        {% set rel = adapter.get_relation(database='FD_LANDING_TABLES_PROD', schema='FDS_STAGING_LYNX_PARQUET', identifier='HIST_LYNX_ESTATE_PARCEL') %}  
        {% if rel %}
            {% set query %}
                create or replace table {{ rel.database | lower }}.{{ rel.schema | lower }}.{{ rel.identifier | lower }}__backup_start_{{ make_ts() }} as 
                select * from {{ rel.database | lower }}.{{ rel.schema | lower }}.{{ rel.identifier | lower }};
            {% endset %}
            {% do run_query(query) %}
        {% endif %}

        {% set rel = adapter.get_relation(database='FD_LANDING_TABLES_PROD', schema='FDS_STAGING_LYNX_PARQUET', identifier='HIST_LYNX_ESTATE_PARCEL') %}  
        {% if rel %}
            {% set query %}
                create or replace table {{ rel.database | lower }}.{{ rel.schema | lower }}.{{ rel.identifier | lower }}__backup_start_{{ make_ts() }} as 
                select * from {{ rel.database | lower }}.{{ rel.schema | lower }}.{{ rel.identifier | lower }};
            {% endset %}
            {% do run_query(query) %}
        {% endif %}

    {% endif %}
{% endmacro %}

{% macro check_results_drop_backups(results) %}
    {% if execute and target.name != 'prod' %}
        {% for r in results %}
            {#/* Filter results for the test of interest */#}
            {#
            #}
            {% if r.node.resource_type == 'test' and r.node.test_metadata.name == 'not_null' and 'REF_LYNX_ESTATE_PARCEL_ESTATE_VISIBLE_CODE' in r.node.test_metadata.kwargs.model %}
                {% if r.status == 'pass' %}
                    {% do log('ESTATE_VISIBLE_CODE is not null - dropping backups.', True) %}

                    {% set rel = adapter.get_relation(database='FD_LANDING_TABLES_PROD', schema='FDS_STAGING_LYNX_PARQUET', identifier='HIST_LYNX_ESTATE_PARCEL') %}
                    {% set drop_model_query %}
                        drop table if exists {{ rel.database | lower }}.{{ rel.schema | lower }}.{{ rel.identifier | lower }}__backup_start_{{ make_ts() }};
                    {% endset %}
                    {% do run_query(drop_model_query) %}

                    {% set rel = adapter.get_relation(database='FD_LANDING_TABLES_PROD', schema='FDS_STAGING_LYNX_PARQUET', identifier='HIST_LYNX_ESTATE_PARCEL') %}  
                    {% set drop_backup_query %}
                        drop table if exists {{ rel.database | lower }}.{{ rel.schema | lower }}.{{ rel.identifier | lower }}__backup_start_{{ make_ts() }};
                    {% endset %}
                    {% do run_query(drop_backup_query) %}

                    {% do log('Backups dropped.', True) %}

                {% else %}

                    {% do log('ESTATE_VISIBLE_CODE is null - backups left as is. Additionally creating another backup of FD_LANDING_TABLES_PROD.FDS_STAGING_LYNX_PARQUET.HIST_LYNX_ESTATE_PARCEL.', True) %}

                    {% set rel = adapter.get_relation(database='FD_LANDING_TABLES_PROD', schema='FDS_STAGING_LYNX_PARQUET', identifier='HIST_LYNX_ESTATE_PARCEL') %}
                    {% set create_model_query %}
                        create or replace table {{ rel.database | lower }}.{{ rel.schema | lower }}.{{ rel.identifier | lower }}__backup_end_{{ make_ts() }} as 
                        select * from {{ rel.database | lower }}.{{ rel.schema | lower }}.{{ rel.identifier | lower }};
                    {% endset %}
                    {% do run_query(create_model_query) %}

                    {% set rel = adapter.get_relation(database='FD_LANDING_TABLES_PROD', schema='FDS_STAGING_LYNX_PARQUET', identifier='HIST_LYNX_ESTATE_PARCEL') %}  
                    {% set create_backup_query %}
                        create or replace table {{ rel.database | lower }}.{{ rel.schema | lower }}.{{ rel.identifier | lower }}__backup_end_{{ make_ts() }} as 
                        select * from {{ rel.database | lower }}.{{ rel.schema | lower }}.{{ rel.identifier | lower }};
                    {% endset %}
                    {% do run_query(create_backup_query) %}
                {% endif %}
            {% endif %}
        {% endfor %}
    {% endif %}
{% endmacro %}
```
