import sys
import re

def drop_schema(notebook_path):
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

    for env in ['d', 't', 'p']:
        drop = notebook_str[0:match.span()[0]]+ f"DROP SCHEMA IF EXISTS dap_{env}_gold_dataenheden.{match.group(1)};"

        with open(notebook_path.replace('.sql', f'_{env}_temp.sql'), 'w') as temp_notebook:
            temp_notebook.write(drop)

if __name__ == '__main__':
    drop_schema(sys.argv[1])

