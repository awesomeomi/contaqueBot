import json
import spacy
from spacy.matcher import PhraseMatcher
from db import execute_query

# Load spaCy English model
nlp = spacy.load('en_core_web_sm')

# Load schema mappings
with open('schema_mapping.json', 'r') as f:
    schema = json.load(f)

tables_mapping = schema['tables']
columns_mapping = schema['columns']
values_mapping = schema['values']
aggregations_mapping = schema['aggregations']

# Initialize PhraseMatchers for efficient matching
table_matcher = PhraseMatcher(nlp.vocab, attr="LOWER")
for table_alias in tables_mapping.keys():
    table_matcher.add("TABLE", [nlp(table_alias)])

value_matcher = PhraseMatcher(nlp.vocab, attr="LOWER")
for value, data in values_mapping.items():
    column = data['column']
    for alias in data['aliases']:
        value_matcher.add(value.upper(), [nlp(alias.lower())])

aggregation_matcher = PhraseMatcher(nlp.vocab, attr="LOWER")
for agg_aliases in aggregations_mapping.values():
    for alias in agg_aliases:
        aggregation_matcher.add("AGGREGATION", [nlp(alias)])

def parse_user_input(user_input):
    doc = nlp(user_input)
    
    print(f"User input: {user_input}")
    
    tables_identified = []
    columns = []
    conditions = []
    aggregation = None
    resource = None
    resource_value = None
    
    # Match tables
    matches = table_matcher(doc)
    print(f"Table matcher matches: {matches}")
    if matches:
        for match_id, start, end in matches:
            span = doc[start:end]
            table = tables_mapping.get(span.text.lower())
            if table:
                tables_identified.append(table)
                print(f"Identified table: {table}")
    
    # Match aggregations
    matches = aggregation_matcher(doc)
    print(f"Aggregation matcher matches: {matches}")
    if matches:
        match_id, start, end = matches[0]
        span = doc[start:end]
        for agg, aliases in aggregations_mapping.items():
            if span.text.lower() in aliases:
                aggregation = agg.upper()
                print(f"Identified aggregation: {aggregation}")
                break
    
    # Match values
    matches = value_matcher(doc)
    print(f"Value matcher matches: {matches}")
    for match_id, start, end in matches:
        span = doc[start:end]
        value = span.text.lower()
        print(f"Found value match: {value}")
        if value in values_mapping:
            column = values_mapping[value]['column']
            conditions.append(f"{column} = '{value.upper()}'")
            print(f"Added condition: {column} = '{value.upper()}'")
        else:
            resource = span.text.lower()
            resource_value = span.text.lower()
            print(f"Identified resource: {resource}, value: {resource_value}")

    # Additional handling for specific resources like campaigns
    if 'campaign' in user_input.lower():
        parts = user_input.lower().split('campaign')
        if len(parts) > 1:
            resource_value = parts[-1].strip()  # Campaign name (e.g., "testaudit")
            print(f"Identified resource_value for campaign: {resource_value}")
    
    parsed_output = {
        'tables': tables_identified,
        'columns': columns,
        'conditions': conditions,
        'aggregation': aggregation,
        'resource': resource,
        'resource_value': resource_value
    }
    
    print(f"Parsed output: {parsed_output}")
    
    return parsed_output

def generate_query(parsed_input):
    tables = parsed_input.get('tables', [])
    conditions = parsed_input.get('conditions', [])
    aggregation = parsed_input.get('aggregation')
    resource_value = parsed_input.get('resource_value')
    
    if not tables:
        return "SELECT 'Sorry, I could not identify the table to query.';"
    
    if len(tables) > 1:
        primary_table = tables[1]  # Table to resolve ID from (e.g., ct_campaign)
        secondary_table = tables[0]  # Table to run the final query on (e.g., ct_dispositions)
        
        if resource_value:
            id_table = primary_table
            id_column = get_id_column(id_table)
            id_value = resolve_id(id_table, resource_value)
            if id_value:
                conditions.append(f"{get_id_column(secondary_table)} = {id_value}")
            else:
                return "Sorry, could not find the resource."
        
        where_clause = ''
        if conditions:
            where_clause = ' WHERE ' + ' AND '.join(conditions)
        
        if aggregation == 'COUNT':
            query = f"SELECT COUNT(*) FROM {secondary_table}{where_clause};"
        else:
            query = f"SELECT * FROM {secondary_table}{where_clause};"
    else:
        table = tables[0]
        where_clause = ''
        if conditions:
            where_clause = ' WHERE ' + ' AND '.join(conditions)
        
        if aggregation == 'COUNT':
            query = f"SELECT COUNT(*) FROM {table}{where_clause};"
        else:
            query = f"SELECT * FROM {table}{where_clause};"
    
    print(f"Generated query: {query}")
    return query


def resolve_id(table_name, resource_value):
    id_column = get_id_column(table_name)
    query = f"SELECT {id_column} FROM {table_name} WHERE name = '{resource_value}'"
    result = execute_query(query)
    if result:
        return result[0][0]
    return None






def get_id_column(table_name):
    id_column = schema.get('id_columns', {}).get(table_name, 'id')
    print(f"ID column for table '{table_name}': {id_column}")
    return id_column


def format_response(user_query, result):
    entity = None
    for key in tables_mapping.keys():
        if key in user_query.lower():
            entity = tables_mapping[key]
            break

    if not entity:
        return "Sorry, I couldn't determine the entity from your query."

    # Remove 'ct_' prefix and underscores
    entity_cleaned = entity.replace('ct_', '').replace('_', ' ')
    print(f"Cleaned entity: {entity_cleaned}")
    
    # Format the response
    count = result[0][0] if result else 0
    response = f"There {'is' if count == 1 else 'are'} {count} {entity_cleaned if count == 1 else entity_cleaned + 's'}"
    print(f"Formatted response: {response}")
    
    return response


# def generate_query(parsed_input):
#     table = parsed_input.get('table')
#     conditions = parsed_input.get('conditions', [])
#     aggregation = parsed_input.get('aggregation')
#     resource = parsed_input.get('resource')
#     resource_value = parsed_input.get('resource_value')
    
#     if not table:
#         return "SELECT 'Sorry, I could not identify the table to query.';"
    
#     print(f"Generating query for table: {table}")
    
#     # Handle ID resolution if needed
#     if resource and resource_value:
#         id_table = tables_mapping.get(resource.lower())
#         if id_table:
#             id_column = get_id_column(id_table)
#             id_value = resolve_id(id_table, resource_value)
#             if id_value:
#                 conditions.append(f"{id_column} = {id_value}")
    
#     where_clause = ''
#     if conditions:
#         where_clause = ' WHERE ' + ' AND '.join(conditions)
#         print(f"Where clause: {where_clause}")
    
#     if aggregation == 'COUNT':
#         query = f"SELECT COUNT(*) FROM {table}{where_clause};"
#     else:
#         query = f"SELECT * FROM {table}{where_clause};"
    
#     print(f"Generated query: {query}")
#     return query
