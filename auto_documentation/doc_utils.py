import os
import sys
import re
import yaml
import streamlit as st
sys.path.append(r'/Users/eeilstein/Desktop/Code/Python/Repos/RepoReader')  # Provide the absolute path to project_root

from general_utils import (
    extract_repo_name,
    is_repo_cloned,
    clone_github_repo,
    )

def get_sql_files(directory):
    """
    Return a list of full paths of all SQL files in the specified directory and its subdirectories.
    """
    sql_files = []

    for dirpath, dirnames, filenames in os.walk(directory):
        for filename in filenames:
            if filename.endswith('.sql'):
                sql_files.append(os.path.join(dirpath, filename).replace(f'{directory}/', ''))

    return sql_files


def display_sql_files(directory):
    """
    Display a selectbox with SQL files from the given directory
    and print the selected file name.
    """
    # Get all SQL files
    sql_files = get_sql_files(directory)

    # Display the files in a selectbox
    selected_file_path = st.selectbox('Choose an SQL file:', [None] + sorted(sql_files))
    selected_file = os.path.basename(selected_file_path)

    if selected_file:
        # Print the selected file name (without the path)
        st.markdown(f"You selected: <b>`{selected_file}`</b>", unsafe_allow_html=True)
    return selected_file, selected_file_path


def extract_active_sources_refs(sql_text, remove_commented=False):
    # Find all instances of source and ref
    sources_matches = re.finditer(r"{{\s*source\('([^']+)',\s*'([^']+)'\)\s*}}", sql_text)
    refs_matches = re.finditer(r"{{\s*ref\(\s*'([^']+)'\s*\)\s*}}", sql_text)

    if remove_commented:
        # Extract the relevant values from the matches and filter out those that are commented
        sources_list = [match.group(2) for match in sources_matches if
                        not re.search(r"--\s*{{\s*source", sql_text[max(0, match.start() - 10):match.end()])]
        refs_list = [match.group(1) for match in refs_matches if
                     not re.search(r"--\s*{{\s*ref", sql_text[max(0, match.start() - 10):match.end()])]

    else:
        # st.warning("WARNING: The SQL file contains commented sources and/or refs. ")
        # Extract the relevant values from the matches
        sources_list = [match.group(2) for match in sources_matches]
        refs_list = [match.group(1) for match in refs_matches]

    return {'sources': list(set(sources_list)), 'refs': list(set(refs_list))}

def remove_comments_from_sql(sql_text):
    # Remove single-line comments
    sql_without_single_line_comments = re.sub(r"--.*$", "", sql_text, flags=re.MULTILINE)

    # Remove multi-line comments
    sql_no_comments = re.sub(r"/\*.*?\*/", "", sql_without_single_line_comments, flags=re.DOTALL)

    # Replace any sequence of three or more newlines with just two newlines
    sql_cleaned = re.sub(r'\n{3,}', '\n\n', sql_no_comments.strip())

    return re.sub(r'\n{2,}', '\n', sql_cleaned.strip())



def get_documentation_from_path(full_path):

    # get file name
    file_name = os.path.basename(full_path)

    # Extract the directory path
    dir_path = os.path.dirname(full_path)

    try:
        # Iterate over all files in the directory
        for filename in os.listdir(dir_path):
            # Check if the filename matches the pattern "*__models.yml"
            if re.match(r".*__models\.yml$", filename):
                doc_path = os.path.join(dir_path, filename)
                doc, doc_status = get_doc_from_yml(doc_path, file_name)
                return {'doc': doc, 'doc_status': doc_status}

        return None  # Return None if no matching file is found

    except FileNotFoundError:
        st.warning(f"Error: Directory '{dir_path}' does not exist.")
        return None

def get_doc_from_yml(doc_path, file_name):
    # file name without extension
    file_name = file_name.split('.')[0]

    yml_content = read_file(doc_path)
    # convert to dict
    yml_dict = yaml.load(yml_content, Loader=yaml.FullLoader)
    # get the file's doc in the yml
    model = [model for model in
             yml_dict.get('models')
             if model.get('name') == file_name][0]
    doc_status = {
        "description_length": len(model.get('description')),
        "columns": len(model.get('columns')),
    }
    st.warning(f'Documentation Status for {file_name}')
    st.write(doc_status)

    # get the file's doc in the yml
    return model, doc_status



def get_documentation_from_dependencies(deps, local_repo_path):
    """
    Get the documentation for the dependencies
    by searching for the __models.yml file in the same directory
    :param deps: dict of dependencies
    :param local_repo_path: str
    :return: dict of documentation by type
    """
    # check for documentation
    # get the documentation for the refs
    ref_docs = {}
    if len(deps['refs']) > 0:
        for dep in deps['refs']:
            ref_docs[dep] = get_documentation_from_path(get_path_from_table_name(f'{dep}.sql', local_repo_path))

    # get the documentation for the sources
    src_docs = {}
    if len(deps['sources']) > 0:
        for dep in deps['sources']:
            src_docs[dep] = get_documentation_from_path(get_path_from_table_name(f'{dep}.sql', local_repo_path))

    return {'refs': ref_docs, 'sources': src_docs}


def read_file(file_path):
    with open(file_path, 'r') as f:
        return f.read()


def get_path_from_table_name(filename, root_dir="."):
    """
    Search for a file recursively in the given root directory.

    Args:
    - filename (str): The name of the file to search for.
    - root_dir (str): The root directory to start the search from. Default is the current directory.

    Returns:
    - str: The full path to the file if found, or a message indicating it wasn't found.
    """
    for dirpath, dirnames, filenames in os.walk(root_dir):
        if filename in filenames:
            full_path = os.path.join(dirpath, filename)
            st.success(f"`{filename}` found in: `{full_path}`")
            return full_path

    return f"File '{filename}' not found in project."
