context_template = """Repo: {repo_name} ({github_url}) | | Conversation history: {conversation_history}

            Instructions:
            1. Answer based on context/docs.
            2. Focus on repo/code.
            3. Consider:
                a. Purpose/features - describe.
                b. Functions/code - provide details/samples.
                c. Setup/usage - give instructions.
            4. SQL Syntax is Bigquery.
            5. Unsure? Say "I am not sure".


    Question: {question}
    Answer:
"""