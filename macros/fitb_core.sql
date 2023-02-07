{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}

{% macro set_query_tag() -%}
  {% set new_query_tag = model.name %} 
  {% if new_query_tag %}
    {% set original_query_tag = get_current_query_tag() %}
    {{ log("Setting query_tag to '" ~ new_query_tag ~ "'. Will reset to '" ~ original_query_tag ~ "' after materialization.") }}
    {% do run_query("alter session set query_tag = '{}'".format(new_query_tag)) %}
    {{ return(original_query_tag)}}
  {% endif %}
  {{ return(none)}}
{% endmacro %}

{%- macro get_merge_sql(target, source, unique_key, dest_columns, predicates=none) -%}
  {{ adapter.dispatch('get_merge_sql')(target, source, unique_key, dest_columns, predicates) }}
{%- endmacro -%}

{%- macro snowflake__get_merge_sql(target, source, unique_key, dest_columns, predicates) -%}
    {%- set predicates = [] if predicates is none else [] + predicates -%}   
    {%- set src_col_lst = [] -%}
    {%- set tgt_col_lst = [] -%}
    {%- set hash_col_src = [] -%}
    {%- set hash_col_tgt = [] -%}
    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
    {%- set additional_matching_columns = dest_columns | map(attribute="name") if config.get('additional_matching_columns', []) == [] else  config.get('additional_matching_columns') -%}
    {%- set merge_exclude_columns = config.get('merge_exclude_columns', [])  -%}
    {#%- set update_columns = dbt.get_additional_matching_columns('additional_matching_columns', 'merge_exclude_columns', dest_columns) -%#}
    {%- set sql_header = config.get('sql_header', none) -%}
    {%- set conditions = [] -%}
    {%- set conditions = config.get('condition') -%}
    {#%- set conditions = conditions|replace("src.","DBT_INTERNAL_SOURCE.") -%#}
    {#%- set conditions = conditions|replace("tgt.","DBT_INTERNAL_DEST.") -%#}
    {%- set scd_type = config.get('scd_type', 1) -%}

    {%- if unique_key -%}
        {%- if unique_key is sequence and unique_key is not mapping and unique_key is not string -%}
            {%- for key in unique_key -%}
                {%- set this_key_match -%}
                    DBT_INTERNAL_SOURCE.{{ key }} = DBT_INTERNAL_DEST.{{ key }}
                {%- endset -%}
                {%- do predicates.append(this_key_match) -%}

                {%- set src_col -%}
                    DBT_INTERNAL_SOURCE.{{key}}
                {%- endset %}
                {%- do src_col_lst.append(src_col) -%}

                {%- set tgt_col -%}
                    DBT_INTERNAL_DEST.{{key}}
                {%- endset %}
                {%- do tgt_col_lst.append(tgt_col) -%} 

            {%- endfor -%}
        {%- else -%}
            {%- set unique_key_match -%}
                DBT_INTERNAL_SOURCE.{{ unique_key }} = DBT_INTERNAL_DEST.{{ unique_key }}
            {%- endset -%}
            {%- do predicates.append(unique_key_match) -%}

            {%- set src_col -%}
                DBT_INTERNAL_SOURCE.{{unique_key}}
            {%- endset %}
            {%- do src_col_lst.append(src_col) -%}

            {%- set tgt_col -%}
                DBT_INTERNAL_DEST.{{unique_key}}
            {%- endset %}            
            {%- do tgt_col_lst.append(tgt_col) -%}
        {%- endif -%}
    {%- else -%}
        {%- do predicates.append('FALSE') -%}
        {%- do src_col_lst.append('FALSE') -%}
        {%- do tgt_col_lst.append('FALSE') -%}
    {%- endif -%}

    
    {{ sql_header if sql_header is not none }}

    {%- for column_name in dest_columns | map(attribute="name") -%}
            {%- if column_name in (merge_exclude_columns) %}
                {{skip}}
            {%- else %}    
                {% set concat_col_src %}
                DBT_INTERNAL_SOURCE.{{column_name}}
                {% endset %}

                {% set concat_col_tgt %}
                DBT_INTERNAL_DEST.{{column_name}}
                {% endset %}

                {%- do hash_col_src.append(concat_col_src) -%}
                {%- do hash_col_tgt.append(concat_col_tgt) -%}
            {%- endif -%}
            
    {%- endfor %}

    {% if scd_type == '1' %}
        merge into {{ target }} as DBT_INTERNAL_DEST
            using {{ source }} as DBT_INTERNAL_SOURCE
            on {{ predicates | join(' and ') }}
    
        {%- if unique_key %}
        when matched 
            {%- if conditions -%}
                {%- for cond in conditions -%}
                    {%- set conds -%}
                        {{ cond }}
                    {%- endset -%}
                    {%- set conds = conds|replace("src.","DBT_INTERNAL_SOURCE.") -%}
                    {%- set conds = conds|replace("tgt.","DBT_INTERNAL_DEST.") -%}
                    {%- set conds = ' and '+ conds -%}
                        {{ conds }}
            {%- endfor -%}
            {%- endif %}
        then update set
        {% for column_name in additional_matching_columns -%}
            {%- if (column_name) == 'Z_LST_UPDT' %}
             {{ column_name }} = current_timestamp()
            {%- elif (column_name) == 'Z_LOAD' %}
                        {{skip}}
            {%- else %}
                      {{ column_name }} = DBT_INTERNAL_SOURCE.{{ column_name }}
            {%- endif -%}  
                    {%- if not loop.last and (column_name) != 'Z_LOAD' -%}, {%- endif -%}
        {%- endfor -%}
        {%- endif %} 
                
        when not matched then insert
            ({{ dest_cols_csv }})
        values
        (
        {%- for column_name in dest_columns | map(attribute="name") -%}
            {{ column_name }}
            {%- if not loop.last -%}, {%- endif -%}
        {%- endfor %}
        )
    {% endif %}

    {%- if scd_type =='2' -%}
       
    BEGIN ;
        update {{target}} DBT_INTERNAL_DEST 
            set D_END_DATE = current_timestamp(),
                F_CURRENT_FLAG = 'N'
        from {{source}} as DBT_INTERNAL_SOURCE
        where {{ predicates | join(' and ') }}
        {% if conditions %}
                {%- for cond in conditions -%}
                    {%- set conds -%}
                        {{ cond }}
                    {%- endset -%}
                    {%- set conds = conds|replace("src.","DBT_INTERNAL_SOURCE.") -%}
                    {%- set conds = conds|replace("tgt.","DBT_INTERNAL_DEST.") -%}
                    {%- set conds = ' and '+ conds -%}
                        {{ conds }}
            {%- endfor -%}
        {%- endif %}
        and DBT_INTERNAL_DEST.F_CURRENT_FLAG = 'Y'
        and hash({{hash_col_src| join(', ') }}) <> hash({{hash_col_tgt| join(', ') }})
        ;

        insert into {{target}}
        ({{ dest_cols_csv }})
        select 
        {%-for column_name in dest_columns | map(attribute="name") %}
            DBT_INTERNAL_SOURCE.{{ column_name }}
            {%- if not loop.last -%}, {% endif %}
        {%- endfor %}
        from {{ source }} as DBT_INTERNAL_SOURCE, 
        (select * from {{target}} as DBT_INTERNAL_DEST
         qualify row_number() over (partition by {{ tgt_col_lst | join(' , ') }}  order by DBT_INTERNAL_DEST.D_END_DATE desc)  = 1 
        ) as DBT_INTERNAL_DEST
        where {{ predicates | join(' and ') }}
        and hash({{hash_col_src| join(', ') }}) <> hash({{hash_col_tgt| join(', ') }})
		;
    
        insert into {{target}} 
        ({{ dest_cols_csv }})
        select 
        {%- for column_name in dest_columns | map(attribute="name") %}
            DBT_INTERNAL_SOURCE.{{ column_name }}
            {%- if not loop.last -%}, {% endif %}
        {%- endfor %}
        from {{ source }} as DBT_INTERNAL_SOURCE
        where ({{ src_col_lst | join(' , ') }}) not in
        (select {{ tgt_col_lst | join(' , ') }} from {{target}} as DBT_INTERNAL_DEST ) 
		;
            
        COMMIT; 
    {%- endif -%}
{%- endmacro -%}
