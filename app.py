import streamlit as st
from code_reader.repo_reader import main as repo_reader_main
from auto_documentation.auto_doc import main as auto_doc_main


apps_dict = {
    "Home": None,
    "Ask about the code": repo_reader_main,
    "Auto-create Documentation": auto_doc_main,
}

def main():
    st.title("Welcome to Agent BiB!")
    st.subheader("You friendly neighborhood Business Intelligence Bot 🤖")
    selected_reason = st.selectbox("How can I help you?", list(apps_dict.keys()))
    if selected_reason:
        st.session_state.current_page = selected_reason
    # render selected page
    try:
        apps_dict[selected_reason]()
    except TypeError:
        pass

if __name__ == "__main__":
    main()