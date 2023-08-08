from .doc_utils import (
    extract_repo_name,
    is_repo_cloned,
    clone_github_repo,
    get_sql_files,
    display_sql_files,
    extract_active_sources_refs,
    remove_comments_from_sql,
    get_documentation_from_path,
    get_documentation_from_dependencies,
    read_file,
    get_path_from_table_name,
)
from .doc_config import (github_url as GITHUB_URL, stat_path_repos as STAT_PATH_REPOS)
from .doc_llm import (get_generated_doc)

import os
import streamlit as st

def main():

    st.title("Auto Documentation")
    st.subheader("Select a repo and a file to document")

    input_url = st.text_input("GitHub URL", GITHUB_URL)
    github_url = input_url.strip()

    repo_name = extract_repo_name(github_url)
    repo_local_path = os.path.join(STAT_PATH_REPOS, repo_name)
    selected_file, selected_file_path = display_sql_files(repo_local_path)
    file_full_path = os.path.join(repo_local_path, selected_file_path)

    # set 2 columns
    is_remove_commented_source_col, is_remove_commented_code_col = st.columns(2)
    is_remove_commented_source = is_remove_commented_source_col.checkbox("Remove commented sources and refs", value=True)
    is_remove_commented_code = is_remove_commented_code_col.checkbox("Remove commented code", value=False)

    # display the file content and dependencies (sources and refs)
    if selected_file:
        selected_file_content = read_file(file_full_path)

        cleaned_file_content = remove_comments_from_sql(selected_file_content)
        st.code(cleaned_file_content if is_remove_commented_code else selected_file_content, language="sql")
        dependencies = extract_active_sources_refs(cleaned_file_content, is_remove_commented_source)

        st.subheader("Dependencies")
        st.write(dependencies)

        # iterate over the dependencies and display the documentation
        docs = get_documentation_from_dependencies(dependencies, repo_local_path)
        st.subheader("Documentation of dependencies")
        st.write(docs)

        # get response from LLM
        model_input = {"name": selected_file, "code": cleaned_file_content}
        response = get_generated_doc(model_input, docs)
        st.write(response)


if __name__ == "__main__":
    main()
