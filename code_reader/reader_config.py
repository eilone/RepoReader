import sys
sys.path.append(r'/Users/eeilstein/Desktop/Code/Python/Repos/RepoReader')  # Provide the absolute path to project_root

from general_config import (
    github_url as GITHUB_URL,
    stat_path_repos as STAT_PATH_REPOS,
    LLM_TEMPERATURE as _LLM_TEMPERATURE,
    LLM_MODEL_NAME as _LLM_MODEL_NAME,
        )

# GitHub URL
from general_config import (github_url)

# Num Relevant Docs
NUM_SOURCE_DOCS = 14

# want to reset history?
is_reset_history = False

# Reset Chroma DB? // Usually False
HARD_RESET_DB = False
# ============================== #


# colors
# ============================== #

WHITE = "\033[37m"
GREEN = "\033[32m"
PURPLE = "\033[35m"
RED = "\033[31m"
RESET_COLOR = "\033[0m"
GREY = "\033[90m"


# LLM vars
# ============================== #

LLM_TEMPERATURE = _LLM_TEMPERATURE
LLM_MODEL_NAME = _LLM_MODEL_NAME

# where to store the cloned repos
LOCAL_PATH = STAT_PATH_REPOS
GIT_URL = GITHUB_URL
