import json
import spacy
from db import execute_query, TableNotFoundError

nlp = spacy.load('en_core_web_sm')

with open('latest_json.json', 'r') as f:
    config = json.load(f)



intents = config.get('intents', {})
resources = config.get('resources', {})
conditions = config.get('conditions', {})

def tokenize_input(user_input):
    doc = nlp(user_input)
    tokens = [token.text for token in doc]
    print(f"tokens: {tokens}")
    return tokens

def match_intents(tokens):
    matched_intents = []

    token_set = set(tokens)
    
    for intent, details in intents.items():

        keywords = details.get('keywords', [])

        keywords_set = set(keywords)
        
        if keywords_set.issubset(token_set):
            matched_intents.append((intent, details))
        else:
            print(f"Intent '{intent}' not matched.")
    
    return matched_intents

def match_resources(tokens):
    matched_resources = []
    
    for i, token in enumerate(tokens):
        for resource_name, resource_details in resources.items():
            if resource_name == token:
                resource_copy = resource_details.copy()  

                if i > 0:
                    resource_copy['secondary_column_value'] = tokens[i - 1]  
                else:
                    resource_copy['secondary_column_value'] = None  

                matched_resources.append((resource_name, resource_copy))
    
    return matched_resources


def match_conditions(tokens):
    matched_condition = []
    for condition_name, condition_details in conditions.items():
        if condition_name in tokens:
            matched_condition.append((condition_name, condition_details))
    return matched_condition

def check_relationship(tokens):
    if "in" in tokens:
        return True

def format_response( matched_resources, result):
    if not result:
        return "No results found for your query."

    print("matched_resources:", matched_resources)
    count_value = result[0][0]
    if count_value>-1:
        entity = matched_resources[0][0]
        return f"There are {count_value} {entity}."
    
def format_response_for_memory( matched_resources, result):
    if not result:
        return "No results found for your query"
    
    count_value = result[0][0]
    if count_value>-1:
        entity = matched_resources[0][0][0]
        return f"There are {count_value} {entity}"

def foreign_key_column(primary_resource, secondary_resource):
    secondary_table = primary_resource['table']

    relationships = {
        "ct_domain" : "domainid",
        "ct_user_group" : "usergroupid",
        "ct_campaign" : "campid",
    }

    if secondary_table in relationships:
        return relationships[secondary_table]
    else:
        raise ValueError(f"No foreign key column found for: {secondary_table}")

def build_query(matched_intents, resources, conditions, relationship):

    print("matched intents:", matched_intents)
    print("resources:", resources)
    print("conditions:", conditions)
    print("relationship:", relationship)
    if not matched_intents:
        raise ValueError("No matched intents found to build the query.")
    
    intent_name, intent_details = matched_intents[0]

    if isinstance(resources[0], list) and len(resources[0]) == 1:
        resources = resources[0]
    
    if 'agents' in resources[0]:
        campid_query = get_campid_for_agents(resources)
        return campid_query
    
    if relationship:
        primary_resource_name, primary_resource_detail = resources[1]  
        secondary_resource_name, secondary_resource_detail = resources[0]  

        foreign_key_columns = foreign_key_column(primary_resource_detail, secondary_resource_detail)

        query = intent_details['conditions']['with_relationship'].format(
            primary_table=secondary_resource_detail['table'],
            foreign_key_column = foreign_key_columns,
            secondary_table=primary_resource_detail['table'],
            secondary_value=primary_resource_detail['secondary_column_value']
        )
    else:
        if len(resources[0]) == 2:
            primary_resource_name, primary_resource_detail = resources[0]
            query = intent_details['conditions']['simple'].format(
                table=primary_resource_detail['table']
            )
        else:
            raise ValueError(f"Resource format incorrect: {resources[0]}")
        
    if conditions and relationship:
        for condition_name, condition_details in conditions:
            query += f" AND {condition_details['column']} = '{condition_details['value']}'"
    if conditions and not relationship:
        for condition_name,condition_details in conditions:
            query += f" WHERE {condition_details['column']} = '{condition_details['value']}'"

    print("Final query:", query)
    return query

def get_campid_for_agents(resources):
    try:
        campaign_name = resources[1][1]['secondary_column_value']

        campid_query = f"SELECT id FROM ct_campaign WHERE name = '{campaign_name}'"
        print("campid_query:", campid_query)
        
        campid_result = execute_query(campid_query)
        
        if campid_result:
            campid = campid_result[0][0]
            live_agents_query = f"SELECT COUNT(*) FROM ct_live_agents_{campid}"
            print(f"Final live agents query: {live_agents_query}")
            return live_agents_query
        else:
            raise TableNotFoundError(f"Campaign '{campaign_name}' not found.")
    
    except IndexError:
        raise TableNotFoundError("No campaign specified or invalid campaign format.")