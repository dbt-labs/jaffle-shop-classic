import yaml
import sys

def has_primary_key(columns):
    #check each
    for column in columns: # for snowflake schema file, primary key would be under model/column/constraint/type:
        constraints = column.get('constraints', [])
        for constraint in constraints:
            if constraint.get('type') == 'primary_key':
                return True
    return False

def check_primary_keys(file_paths):
    for file_path in file_paths:
        with open(file_path, 'r') as file:
            data = yaml.safe_load(file)
            models = data['models']
            for model in models:
                model_name = model['name']

                columns = model.get('columns', [])

                has_keys = has_primary_key(columns)
            
                if has_keys == False:
                    print(f'No primary key fround for model: {model_name}')
                    sys.exit(6) #exit with anything other than 0 for a fail
    print('All models include at least one primary key')
    sys.exit(1)

# Paths to any schema files in the repo to be checked
file_paths = ['models/staging/schema.yml', 'models/schema.yml']
check_primary_keys(file_paths)