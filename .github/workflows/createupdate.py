import sys
import re

def check_format_and_prepare_notebook(notebook_path):
    """ Takes a notebook path and an environment letter.
    Prepares the notebook to be executed against gold_dataenheden schema in the environment
    """
    with open(notebook_path) as notebook:
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
        with open(notebook_path.replace('.sql', f'_{env}_temp.sql'), 'w') as temp_notebook:
            temp_notebook.write(re.sub(r"dap_[dtp]_", f"dap_{env}_", temp_view_str))

if __name__ == '__main__':
    check_format_and_prepare_notebook(sys.argv[1])

