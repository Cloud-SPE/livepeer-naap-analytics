-- Layer-discipline test: enforces the medallion layer contract by
-- walking the dbt graph at compile time and refusing cross-layer skips.
--
-- Contract (docs/design-docs/api-table-contract.md §Enforcement rule 1):
--   api        -> canonical, api_base
--   api_base   -> canonical
--   canonical  -> canonical, staging
--   staging    -> sources only
--   operational -> canonical, operational (live-ops spine)
--
-- The test materialises a row per violation at compile time. If any
-- violations exist the test emits those rows as its SELECT; dbt fails
-- the test when the row count is non-zero.

{%- set layer_of_path -%}
{# Jinja helper captured as a string only to scope variables; the real
   logic lives in the {%- set ... -%} block below. #}
{%- endset -%}

{%- set violations = [] -%}
{%- set layer_rank = {
    'staging':     1,
    'canonical':   2,
    'api_base':    3,
    'api':         4,
    'operational': 3,
} -%}

{%- for node_id, node in graph.nodes.items() -%}
  {%- if node.resource_type == 'model' -%}
    {%- set node_layer = none -%}
    {%- for part in node.fqn -%}
      {%- if part in layer_rank -%}
        {%- set node_layer = part -%}
      {%- endif -%}
    {%- endfor -%}
    {%- if node_layer -%}
      {%- for dep_id in node.depends_on.nodes -%}
        {%- set dep = graph.nodes.get(dep_id) -%}
        {%- if dep and dep.resource_type == 'model' -%}
          {%- set dep_layer = none -%}
          {%- for part in dep.fqn -%}
            {%- if part in layer_rank -%}
              {%- set dep_layer = part -%}
            {%- endif -%}
          {%- endfor -%}
          {%- if dep_layer -%}
            {%- set valid = false -%}

            {# api may ref canonical or api_base, not staging or raw. #}
            {%- if node_layer == 'api' and dep_layer in ['canonical', 'api_base'] -%}
              {%- set valid = true -%}
            {%- elif node_layer == 'api_base' and dep_layer == 'canonical' -%}
              {%- set valid = true -%}
            {%- elif node_layer == 'canonical' and dep_layer in ['canonical', 'staging'] -%}
              {%- set valid = true -%}
            {%- elif node_layer == 'staging' and dep_layer == 'staging' -%}
              {# staging-to-staging (shared helper) is allowed. Sources
                 do not appear in graph.nodes under the model check. #}
              {%- set valid = true -%}
            {%- elif node_layer == 'operational' and dep_layer in ['canonical', 'operational'] -%}
              {%- set valid = true -%}
            {%- endif -%}

            {%- if not valid -%}
              {%- do violations.append(node.name ~ ' (' ~ node_layer ~ ') -> ' ~ dep.name ~ ' (' ~ dep_layer ~ ')') -%}
            {%- endif -%}
          {%- endif -%}
        {%- endif -%}
      {%- endfor -%}
    {%- endif -%}
  {%- endif -%}
{%- endfor -%}

{%- if violations -%}
  select violation from (
    {%- for v in violations %}
      select {{ "'" ~ v | replace("'", "''") ~ "'" }} as violation
      {%- if not loop.last %} union all{% endif %}
    {%- endfor %}
  )
{%- else -%}
  -- No violations. Emit a dummy row that is always filtered out so the
  -- test runs a trivial SELECT and passes.
  select 'none' as violation where 1 = 0
{%- endif -%}
