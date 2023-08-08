# RepoReader
Explore and ask questions about a GitHub code repository using OpenAI's GPT, using **`LangChain`**

## Pre-requisites:
- OpenAI API Key, currently it's available only for paying subscriptions
- Set your key as a `.env` variable:
  - Generate an [OpenAI Key](https://platform.openai.com/account/api-keys)
  - Open a terminal in your desktop
  - run `nano .env`
  - Ctrl + X to exit, Y to save changes
  - In the .env file, write:
    <br> `OPENAI_API_KEY=[KEY]`
      <br>Make sure to replace `[KEY]` with your secret key from OpenAI


## Setup
1. Clone repo
`git clone <reporeader_url>`
2. **Optional** – open and activate a virtual environment
3. Install dependencies <br>
```
// cd to the cloned repo
```

```
 pip install -r requirements.txt
```

5. Run via streamlit
```
streamlit run app.py
```

## Usage
1. Paste the `repo_url` you wish to explore
2. Check the checkbox `Start Chatting`
3. #### _Ask questions!_
4. If it doesn't automatically open the webpage, go in the browser to `http://localhost:8501/` (the port-number itself may vary, 8888 etc.)
5. If you make changes in the repo, **delete it** from the generated path `stat_path_repos` and rerun the script (will re-clone)
6. If you make changes in the config variables such as different embedding, split-size, persist directory etc, check the `Reset DB` Checkbox in the webpage!
7. To view your **Conversation History** you can check the terminal for the printed queries, answers and sources.

### Note:
- The default **file-types** it reads are `['py', 'sql', 'yml', 'md']`. You are welcome to add any other type you'd like!
- The **LLM Model** it uses is set to `gpt-3.5-turbo-16k` as per `2023-07-21`. Hoping to upgrade to `gpt-4` soon!
  
