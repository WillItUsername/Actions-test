import sys
import re
import os.path

def check_format_and_prepare_notebook(folder, domain, notebook_filename):
    """ Takes a split notebook path and an environment letter.
    Prepares the notebook to be executed against gold_dataenheden schema in the environment
    """
    with open(os.path.join(os.path.split(os.path.split(os.path.dirname(__file__))[0])[0],folder, domain, notebook_filename)) as notebook:
        notebook_str = notebook.read()

    # Search for valid create schema statments
    if (matches := list(re.finditer(r"CREATE *SCHEMA *IF *NOT *EXISTS *dap_._gold_dataenheden_sandbox\.(.*);",notebook_str))):
        if len(matches) < 1:
            raise Exception('Did not find a CREATE SCHEMA IF NOT EXISTS statement.')
        elif len(matches) > 1:
            raise Exception('Found multiple CREATE SCHEMA IF NOT EXISTS statements.')
        else:
            match = matches[0]
        
    drop_create = f"DROP SCHEMA IF EXISTS dap_d_gold_dataenheden.{match.group(1)};\nCREATE SCHEMA IF NOT EXISTS dap_d_gold_dataenheden.{match.group(1)};"
    temp_view_str = notebook_str[0:match.span()[0]] + drop_create + notebook_str[match.span()[1]:]
    
    for env in ['d','t','p']:
        with open(f'{env}_temp.sql', 'w') as temp_notebook:
            temp_notebook.write(re.sub(r"dap_[dtp]_", f"dap_{env}_", temp_view_str))

if __name__ == '__main__':
    #Split input path
    temp = sys.argv[1].split('/')
    check_format_and_prepare_notebook(temp[0], temp[1], temp[2])

