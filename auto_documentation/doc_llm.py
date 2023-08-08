import openai

def get_generated_doc(model, deps):
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo-16k",
        messages=[
            {"role": "system", "content": "You are a helpful assistant, an efficient user of tokens that don't waste words."},
            {"role": "user", "content": f"""
            1. I have this undocumented sql model: model name: {model['name']}
            2. Provide a comprehensive description of the model, based on the sql code in GBQ dialect, 
            using the documentation of its dependencies.
            3. The output should be a yml file with the model's documentation, the same format as the dependencies' documentation. 
            
            model content: {model['code']}
            dependencies: {deps}"""}
        ]
    )

    return response