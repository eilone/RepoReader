from .doc_utils import (
    extract_repo_name,
    is_repo_cloned,
    clone_github_repo,
    clone_repo,
    get_sql_files,
    display_sql_files,
    extract_active_sources_refs,
    remove_comments_from_sql,
    get_documentation_from_path,
    get_documentation_from_dependencies,
    read_file,
    get_path_from_table_name,

)
from .doc_config import (
    GITHUB_URL,
    STAT_PATH_REPOS,
    )
from .doc_llm import (get_generated_doc)

import os
import streamlit as st

def main():

    st.title("Auto Documentation")
    st.subheader("Select a repo and a file to document")

    input_url = st.text_input("GitHub URL", GITHUB_URL)
    github_url = input_url.strip()

    repo_name = extract_repo_name(github_url)
    # clone the repo if it doesn't exist
    clone_repo(github_url)

    repo_local_path = os.path.join(STAT_PATH_REPOS, repo_name)
    selected_file, selected_file_path = display_sql_files(repo_local_path)
    file_full_path = os.path.join(repo_local_path, selected_file_path)

    # set 2 columns for the comments removal options
    is_remove_commented_source_col, is_remove_commented_code_col = st.columns(2)
    is_remove_commented_source = is_remove_commented_source_col.checkbox("Remove commented sources and refs", value=True)
    is_remove_commented_code = is_remove_commented_code_col.checkbox("Remove commented code", value=False)

    # display the file content and dependencies (sources and refs)
    if selected_file:
        selected_file_content = read_file(file_full_path)

        cleaned_file_content = remove_comments_from_sql(selected_file_content)
        used_sql_content = cleaned_file_content if is_remove_commented_code else selected_file_content

        st.code(used_sql_content, language="sql")
        dependencies = extract_active_sources_refs(cleaned_file_content, is_remove_commented_source)

        st.subheader("Dependencies")
        st.write(dependencies)

        # iterate over the dependencies and display the documentation
        docs = get_documentation_from_dependencies(dependencies, repo_local_path)
        st.subheader("Documentation of dependencies")
        st.write(docs)

        # set option to use examples or not
        is_use_examples_col, _ = st.columns(2)
        is_use_examples = is_use_examples_col.checkbox("Train LLM on examples", value=False)

        # get response from LLM
        model_input = {"name": selected_file, "code": used_sql_content}
        yml_doc, total_tokens, full_response = get_generated_doc(model=model_input,
                                                  deps=docs,
                                                  is_using_examples=is_use_examples)
        st.code(yml_doc, language="yaml")
        st.write(f'total tokens: {total_tokens}')

        st.subheader("Full response")
        st.write(full_response)


if __name__ == "__main__":
    main()
