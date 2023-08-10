# =============================================== #
# This file contains general utility functions
# =============================================== #

# --------------------------------------------- #
# Imports
# --------------------------------------------- #
import subprocess
import os
import streamlit as st
from general_config import stat_path_repos as STAT_PATH_REPOS
from dotenv import load_dotenv


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


def get_openai_api_key():
    """
    Get the OpenAI API key from the .env file
    :return: the OpenAI API key as a string
    """
    load_dotenv()
    return os.getenv("OPENAI_API_KEY")

### Clone repo

def clone_repo(github_url):
    repo_name = extract_repo_name(github_url)
    local_path = STAT_PATH_REPOS
    _is_repo_cloned = is_repo_cloned(github_url, local_path)
    print(f'[LOG] is repo {repo_name} already cloned? {_is_repo_cloned}')

    # if the repo is already cloned in the static path, then skip cloning. If not, clone it in the static path
    if _is_repo_cloned:
        st.success(f'Repo {repo_name} already cloned')
    else:
        st.warning(f'Cloning repo {repo_name}...')
        clone_github_repo(github_url, os.path.join(local_path, repo_name))
        st.success(f'Repo {repo_name} is now cloned!')

    return repo_name, _is_repo_cloned
