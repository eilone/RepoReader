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
    clone_repo,
    get_openai_api_key,
    llm_model_selection,
    temperature_selection,
    center_column,
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
        print(f"Selected file: {selected_file}")
    return selected_file, selected_file_path


def extract_active_sources_refs(sql_text, remove_commented=False):
    # Find all instances of source and ref
    sources_matches = re.finditer(r"{{source\('([^']+)','([^']+)'\)}}", sql_text.replace(' ', ''))
    refs_matches = re.finditer(r"{{ref\('([^']+)'\)}}", sql_text.replace(' ', ''))

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
    # Remove lines that are entirely single-line comments
    sql_without_single_line_comments = re.sub(r"^\s*--.*$", "", sql_text, flags=re.MULTILINE)

    # Remove lines that are entirely multi-line comments
    # First, split the text into lines
    lines = sql_without_single_line_comments.split('\n')
    # Then, filter out lines that start and end with multi-line comment tokens
    lines = [line for line in lines if not (line.strip().startswith("/*") and line.strip().endswith("*/"))]
    sql_no_comments = '\n'.join(lines)

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
                yml_doc_path = os.path.join(dir_path, filename)
                # check if one of the folders is "archive"
                if 'archive' in yml_doc_path.split('/'):
                    st.warning(f"Found documentation file in archive folder: `{yml_doc_path}`")
                    continue
                st.success(f"Found documentation file: `{yml_doc_path}`")
                doc, doc_status = get_doc_from_yml(yml_doc_path, file_name)
                return {'doc': doc, 'doc_status': doc_status}
        st.warning(f"No documentation file found in directory '{dir_path}'.")
        return {}  # Return None if no matching file is found

    except FileNotFoundError:
        st.warning(f"Error: Directory '{dir_path}' does not exist.")
        return {}


def clean_model(model):
    """
    Remove unnecessary keys from the model dict
    When there is a sub-dict of a column as follows:

    column:
        name: column_name
        description: ""

    Remove the description key and return:

    column:
        name: column_name

    :param model:dict
    :return:model:dict
    """
    for column in model.get('columns'):
        if column.get('description') == "":
            del column['description']
    return model


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
    model = clean_model(model)
    doc_status = get_documentation_status(model)
    doc_score = documentation_score(doc_status)
    st.warning(f'Documentation Status for `{file_name}`: {doc_score[1]} {doc_score[0]}/5')
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
            docs_dict = get_documentation_from_path(get_path_from_table_name(f'{dep}.sql', local_repo_path))
            ref_docs[dep] = docs_dict.get('doc')

    # get the documentation for the sources
    src_docs = {}
    if len(deps['sources']) > 0:
        for dep in deps['sources']:
            docs_dict = get_documentation_from_path(get_path_from_table_name(f'{dep}.sql', local_repo_path))
            src_docs[dep] = docs_dict.get('doc')

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

            # check if one of the folders is "archive"
            if 'archive' in dirnames:
                st.warning(f"Found MODEL file in ARCHIVE folder: `{filename}`")
                continue

            full_path = os.path.join(dirpath, filename)
            st.success(f"`{filename}` found in: `{full_path}`")
            return full_path
    st.warning(f"File '{filename}' not found in project.")
    return f"File '{filename}' not found in project."


def get_documentation_status(doc: dict):
    """
    Get the documentation status for a given doc, based on the following criteria:
    - General description exists
    - Number of columns
    - Number of columns with a description longer than 10 characters
    :param doc: dict
    :return: doc_name - the name of the doc
    general_desc_status - True if the doc has a general description, False otherwise
    num_of_columns - the number of columns in the doc
    num_of_columns_with_long_desc - the number of columns with a description longer than 10 characters
    """
    # get doc name
    doc_name = doc.get('name')

    # Check if the table has a general description longer than 10 characters
    general_desc_status = True if "description" in doc and len(doc["description"]) > 10 else False

    # Get the number of columns
    num_of_columns = len(doc["columns"]) if "columns" in doc else 0

    # Get the number of columns with a description longer than 10 characters
    num_of_columns_with_long_desc = sum(1 for col in doc.get("columns", []) if len(col.get('description', '')) > 10)

    return {'doc_name': doc_name,
            'general_desc_status': general_desc_status,
            'num_of_columns': num_of_columns,
            'num_of_columns_with_long_desc': num_of_columns_with_long_desc}


def documentation_score(doc):
    doc_name, general_desc_status, num_of_columns, num_of_columns_with_long_desc = doc.values()

    # Scoring rules
    score = 0
    if general_desc_status:
        score += 1
    score += 0.5 * num_of_columns
    score += 1 * num_of_columns_with_long_desc

    # Normalize score to fit the 1-5 scale
    max_score = 1 + 1.5 * num_of_columns  # assuming every column has a description longer than 10 characters
    print(f'[LOG] Nane: {doc_name}, Score: {score}, Max Score: {max_score}, general_desc_status: {general_desc_status}')
    normalized_score = round(5 * (score / max_score), 1)

    # Assign an icon based on the score
    icons = {
        0: "❌",  # Red cross (no documentation)
        1: "🔴",  # Red circle (poor documentation)
        2: "🟠",  # Orange circle (below average documentation)
        3: "🟡",  # Yellow circle (average documentation)
        4: "🟢",  # Green circle (good documentation)
        5: "✅"  # Blue circle (excellent documentation)
    }

    return normalized_score, icons[round(normalized_score)]


# create a checkbox to show the file content
def is_show_file_content(root):
    return root.checkbox("Show SQL content", value=False)


# create a checkbox to show dependencies
def is_show_dependencies(root):
    return root.checkbox("Show dependencies", value=False)


# create a checkbox to show full response
def is_show_full_response(root):
    return root.checkbox("Show full response", value=False)


def handle_finish_reason(finish_reason):
    if finish_reason == "success":
        st.sidebar.success("Documentation generated successfully!")
    elif finish_reason == "length":
        st.sidebar.warning("Documentation generated successfully but it was too long!")