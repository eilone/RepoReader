import openai
from .llm_examples import EXAMPLES
from general_utils import (get_openai_api_key,)
from general_config import (
    LLM_TEMPERATURE,
    LLM_MODEL_NAME,
)

PROMPT_SYSTEM = """
You are a DBA and a documentation-generating machine, that gets sql logic of a table and its dependencies, 
and outputs a documentation for the table in YML format
"""


PROMPT_USER = """INSTRUCTIONS: 
1. I have this undocumented sql model: Table name: {model[name]} 
2. Provide a comprehensive description of the table, based on the sql code in GBQ dialect, using the documentation of
its dependencies. 
3. The output should be a yml file with the table's documentation, the same format as the dependencies' 
documentation. meaning: a short general description about the purpose of the data, the granularity, the data source 
and whatever else you see fit. Also, a list of the current table's columns with their descriptions, and a list of 
dependencies with their descriptions. We do not wish to have a copy-paste of the dependencies' documentation, 
but rather an understanding of the SQL logic of the table that is created from the dependencies, and to help you 
understand the table's logic, you can use the dependencies' documentation. 
4. It is important to be descriptive and informative. 
5. THE OUTPUT FORMAT: a yml file. Remember that the table's documentation should be in the same format as the
dependencies' YML documentation, as provided in the context.
DO NOT write a desc on every dependency's column, a short general desc on the dependency is enough. 
6. Return descriptions for ALL the columns in the table. If you are not sure about the description, live an empty str.
{example_prompt[examples_instructions]}
            
              
            
            Table SQL: {model[code]}
            Dependencies YMLs: {deps}
            {example_prompt[examples]}
    """

def get_example_prompt(is_using_examples=True):
    if is_using_examples:
        return {
            "examples_instructions": "7. Learn from the examples provided below on how should a input-output look like",
            "examples": f'Examples: {EXAMPLES},'
        }
    else:
        return {
            "examples_instructions": "",
            "examples": "",
        }


def get_generated_doc(model, deps, is_using_examples=True, **kwargs):
    openai.api_key = get_openai_api_key()
    response = openai.ChatCompletion.create(
        model=kwargs.get('model_name', LLM_MODEL_NAME),
        temperature=kwargs.get('temperature', LLM_TEMPERATURE),
        messages=[
            {"role": "system", "content": PROMPT_SYSTEM},
            {"role": "user", "content": PROMPT_USER.format(
                model=model,
                deps=deps,
                examples=EXAMPLES,
                example_prompt=get_example_prompt(is_using_examples)
               )},
        ]
    )
    yml_doc = response['choices'][0]['message']['content'].strip()
    tokens = response['usage']['total_tokens']

    return yml_doc, tokens, response
