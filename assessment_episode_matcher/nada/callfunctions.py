# https://claude.ai/chat/1e57fa32-2de5-49ad-99de-6e1ad51bb315
import azure.functions as func
import logging

app = func.FunctionApp()


@app.route(route="call_functions")
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # Get the 'functions' parameter from the query string
    functions_param = req.params.get('functions')
    
    if not functions_param:
        return func.HttpResponse(
            "Please provide a 'functions' parameter in the query string.",
            status_code=400
        )
    
    # Parse the functions parameter
    function_names = functions_param.split(',')
    
    # Dictionary of available functions
    available_functions = {
        'greet': greet,
        'add': add,
        'subtract': subtract,
        'multiply': multiply
    }
    
    # Call the requested functions and collect results
    results = []
    for func_name in function_names:
        func_name = func_name.strip()  # Remove any whitespace
        if func_name in available_functions:
            result = available_functions[func_name]()
            results.append(f"{func_name}: {result}")
        else:
            results.append(f"{func_name}: Not found")
    
    # Return the results
    return func.HttpResponse("\n".join(results))

# Example functions to be called
def greet():
    return "Hello, World!"

def add():
    return "2 + 2 = 4"

def subtract():
    return "5 - 3 = 2"

def multiply():
    return "3 * 4 = 12"