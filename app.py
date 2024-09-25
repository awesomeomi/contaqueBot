from flask import Flask, request, jsonify, render_template_string
from flask_cors import CORS, cross_origin
from db import execute_query, TableNotFoundError
# from nlp import parse_user_input, generate_query, format_response
from newnlp import tokenize_input,match_intents, match_conditions,match_resources,check_relationship, build_query, format_response, format_response_for_memory
from memory import BotMemory

app = Flask(__name__)
# CORS(app, resources={"/test_query": {"origins": "*"}})


bot_memory = BotMemory()
# HTML template for the input form
html_form = """
<!doctype html>
<html lang="en">
  <head>
    <title>ContaQue Bot</title>
  </head>
  <body>
    <h1>Test ContaQue Bot</h1>
    <form method="post" action="/test_query">
      <label for="query">Enter your query:</label><br><br>
      <input type="text" id="query" name="query" style="width:500px;"><br><br>
      <input type="submit" value="Submit">
    </form> 

    {% if sql_query %}
    <h2>Generated SQL Query:</h2>
    <p>{{ sql_query }}</p>
    {% endif %}

    {% if result %}
    <h2>Query Result:</h2>
    <pre>{{ result }}</pre>
    {% endif %}
  </body>
</html>
"""

@app.route('/test_query', methods=['POST'])
@cross_origin( 
  origins = '*',  
  methods = ['POST'],  
  headers = None,  
  supports_credentials = True
)
def test_query():
    data = request.get_json()
    user_query = data.get('query')
    print(f"user query: {user_query}")

    # Step 1: Tokenize input
    tokens = tokenize_input(user_query)

    # Step 2: Match intents, resources, and conditions
    matched_intents = match_intents(tokens)
    matched_resources = match_resources(tokens)
    matched_conditions = match_conditions(tokens)
    
    memory_flag = False
    if not matched_resources:
        memory_flag = True
        memory = bot_memory.get_memory()
        if memory['last_resource']:
            print("No new resource mentioned, using last resource from memory")
            matched_resources = [memory['last_resource']]
            print(matched_resources)
    
    # Step 3: Check relationship in the query
    relationship = check_relationship(tokens)

    try:

         # Step 4: Build the SQL query based on the parsed input
        sql_query = build_query(matched_intents, matched_resources, matched_conditions, relationship)

        print(f"Generated SQL Query: {sql_query}")
        # Step 5: Execute the query using your `execute_query` function
        query_result = execute_query(sql_query)

        # Step 6: Format response
        if memory_flag:
            proper_response = format_response_for_memory(matched_resources, query_result)
        else:
            proper_response = format_response(matched_resources, query_result)

        if not memory_flag:
            print("Adding memory")
            bot_memory.update_memory(matched_resources, matched_conditions, matched_intents)

        # Return the results
        if query_result:
            return jsonify(proper_response)
        else:
            return jsonify({"message": "No results found or query could not be processed."})

    except TableNotFoundError as e:
        # Custom message when a table is missing, ask for campaign
        print(f"Custom exception caught: {e}")
        return jsonify({"message": "Please specify a campaign"})
    
    except Exception as e:
        # General error handling
        print(f"An unexpected error occurred: {e}")
        return jsonify({"message": "An unexpected error occurred. Please try again."})

# def test_query():
#     data = request.get_json()
#     user_query = data.get('query')
#     print(f"user query:{user_query}")

#     parsed_input = parse_user_input(user_query)
#     sql_query = generate_query(parsed_input)

#     print(sql_query)

#     response = execute_query(sql_query)

#     properResponse = format_response(user_query, response)
#     if response:
#         return jsonify(properResponse)
#     else:
#         return jsonify("No results found")
#     # response = jsonify(message="hello world")
#     # return response

@app.route('/query', methods=['GET'])
def handle_query():
    data = request.get_json()
    user_query = data.get('query')
    print(f"user query:{user_query}")
    parsed_input = parse_user_input(user_query)
    sql_query = generate_query(parsed_input)
    
    result = execute_query(sql_query)
    
    if result:
        return jsonify(result)
    else:
        return jsonify({"message": "No results found or query could not be processed."})

if __name__ == "__main__":
    app.run(debug=True, port=5001)
