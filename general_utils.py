# =============================================== #
# This file contains general utility functions
# =============================================== #

# --------------------------------------------- #
# Imports
# --------------------------------------------- #
import subprocess
import os
import streamlit as st

# --------------------------------------------- #
# Github functions
# --------------------------------------------- #


#### repo funcs

def extract_repo_name(repo_url):
    # Extract the part of the URL after the last slash and before .git
    repo_name = repo_url.rstrip().rstrip('/').split('/')[-1]
    if len(repo_name) < 2:
        err_msg = f"Invalid repo URL: {repo_url}"
        st.error(err_msg)
        raise ValueError(err_msg)
    if repo_name.endswith('.git'):
        repo_name = repo_name[:-4]  # remove .git from the end
    return repo_name


def is_repo_cloned(repo_url, path_dir):
    repo_name = extract_repo_name(repo_url)
    repo_path = os.path.join(path_dir, repo_name)
    return os.path.isdir(repo_path)


def clone_github_repo(github_url, local_path):
    try:
        subprocess.run(['git', 'clone', github_url, local_path], check=True)
        return True
    except subprocess.CalledProcessError as e:
        print(f"Failed to clone repository: {e}")
        return False

def is_directory_empty(directory):
    """
    Returns True if the given directory is empty, otherwise False.
    """
    return not bool(os.listdir(directory))

