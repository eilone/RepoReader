import sys
sys.path.append(r'/Users/eeilstein/Desktop/Code/Python/Repos/RepoReader')  # Provide the absolute path to project_root

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

LLM_TEMPERATURE = 0.1
LLM_MODEL_NAME = "gpt-3.5-turbo-16k"  # VERIFIED
# LLM_MODEL_NAME = "gpt-3.5-turbo" # VERIFIED
# LLM_MODEL_NAME = "gpt-4-32k" # NOT YET
# LLM_MODEL_NAME = "gpt-4" # NOT YET

# where to store the cloned repos
LOCAL_PATH = 'stat_path_repos'
