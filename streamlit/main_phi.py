import streamlit as st
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import markdown
import google.generativeai as genai

import torch
from transformers import AutoModelForCausalLM, AutoTokenizer, pipeline

torch.random.manual_seed(0)

local_model_directory = "D:/atlas_v2/docker-atlas/phi-3/Phi-3-mini-128k-instruct"

model = AutoModelForCausalLM.from_pretrained(
    local_model_directory, 
    device_map="cuda", 
    torch_dtype="auto", 
    trust_remote_code=True, 
)

tokenizer = AutoTokenizer.from_pretrained(local_model_directory)

st.set_page_config(page_title="e-report", page_icon="🤖")

def main():
    logo = "bcp_logo.png"
    #st.sidebar.image(logo)
    #st.sidebar.title("Navigation")
    page = st.sidebar.selectbox("Choose a page", ["Summary", "Interactive Model"])

    if page == "Summary":
        show_summary_page()
    elif page == "Interactive Model":
        interactive_model_page()

def show_summary_page():
    st.title('Data Quality Report Summary')
    st.caption('Summarize your data quality report log file')

    file = st.file_uploader("Upload a log file to summarize", type=["log", "txt"])
    log_entries = st.text_area("Or paste your log entries here:")
    button_style = """
    <style>
    div.stButton > button:first-child {
        background-color: #EB6325;
        color: white;
        border: none;
        border-radius: 8px;
        padding: 8px 18px;
        font-size: 10px;
        font-weight: bold;
        transition: background-color 0.2s ease, filter 0.2s ease;
    }
    div.stButton > button:hover {
        background-color: white;  /* Slightly darker blue */
        color: #EB6325;
        border: 2px solid #EB6325;
    }
    </style>
    """
    st.markdown(button_style, unsafe_allow_html=True)
    if st.button("Summarize"):
        data_to_summarize = file.getvalue().decode("ISO-8859-1") if file else log_entries
        if data_to_summarize:
            summary = summarize_logs(data_to_summarize)
            st.session_state.summary = summary  # Storing the summary in the session state
            st.markdown(summary, unsafe_allow_html=True)
        else:
            st.error("Please upload a log file or paste some log entries to summarize.")

    if 'summary' in st.session_state and st.session_state.summary:
        st.title("Send Email")
        sender_email = st.text_input("Your Email", key="sender_email")
        receiver_email = st.text_input("Receiver Email", key="receiver_email")
        if st.button("Send Email"):
            result = send_email(sender_email, receiver_email, st.session_state.summary)
            st.success(result)
def summarize_logs(log_entries):
    instructions = """
                    Je vais fournir des journaux de vérification de la qualité des données, voici la structure de chaque journal :
                    timestamp - NIVEAU_DE_LOG - MESSAGE_DE_TRACE
                    où :
                    NIVEAU_DE_LOG : Ce champ accepte 7 valeurs, INFO, ERROR, WARNINGS indiquant l'état du journal.
                    MESSAGE_DE_TRACE : LE RESTE DU MESSAGE

                    En tant qu'expert en journaux, résumez ce qui s'est passé dans les journaux avec détails. Fournissez le résumé dans un paragraphe facile à comprendre.
                    """
    template = f"Instruction:\n{instructions}\nINPUTDATA:\n{log_entries}\nResponse:\n"
    pipe = pipeline(
        "text-generation",
        model=model,
        tokenizer=tokenizer,
    )

    generation_args = {
        "max_new_tokens": 500,
        "return_full_text": False,
        "temperature": 0.0,
        "do_sample": False,
    }
    messages = [
    {"role": "user", "content": template}
    ]

    output = pipe(messages, **generation_args)
    response = output[0]['generated_text']
    return response.text

def send_email(email, receiver_email, response):
    password = "wrobezkyasmarbet"
    subject = "Summary of Metadata Quality Check Failures"
    smtp_server = "smtp.gmail.com"
    smtp_port = 587

    message = MIMEMultipart()
    message['From'] = email
    message['To'] = receiver_email
    message["Subject"] = subject

    content = markdown.markdown(response)
    part = MIMEText(content, "html")
    message.attach(part)
    
    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(email, password)
        server.sendmail(email, receiver_email, message.as_string())
        server.quit()
        return "Email sent successfully"
    except Exception as e:
        server.quit()
        return f"Failed to send email: {e}"

#######################################################33

def interactive_model_page():
    st.title('Interactive Model Session')
    st.caption('Upload a log file and interact directly with the model based on its content')

    button_style = """
    <style>
    div.stButton > button:first-child {
        background-color: #EB6325;
        color: white;
        border: none;
        border-radius: 8px;
        padding: 8px 20px;
        font-size: 10px;
        font-weight: bold;
        transition: background-color 0.2s ease, filter 0.2s ease;
    }
    div.stButton > button:hover {
        background-color: white;  /* Slightly darker blue */
        color: #EB6325;
        border: 2px solid #EB6325;
    }
    </style>
    """
    st.markdown(button_style, unsafe_allow_html=True)

    # Sidebar with a custom-styled button
    with st.sidebar:
        if st.button("Clear chat window", use_container_width=True):
            st.session_state.history = []
            st.rerun()

    
                    
    if "history_user" not in st.session_state:
        st.session_state.history_user = []

    if "history_answer" not in st.session_state:
        st.session_state.history_answer = []

    #chat = model.start_chat(history=st.session_state.history)
    
    # for message in st.session_state.history:
    #     role = "assistant" if message.role == 'model' else message.role
    #     with st.chat_message(role):
    #         st.markdown(message.part[0].text)  
    
    # Handle file upload
    uploaded_file = st.file_uploader("Upload a log file", type=["log", "txt"])
    if uploaded_file is not None:
        file_content = uploaded_file.getvalue().decode("utf-8")
        st.session_state.file_content = file_content
        #st.session_state.history = []
        st.success("File uploaded successfully! You can now ask questions about it.")

    # Check if file content is available for interaction
    if 'file_content' in st.session_state:  
        if prompt := st.chat_input(""):
            
            prompt = prompt.replace('\n', ' \n')
            display_conversation()

            with st.chat_message("user"):
                st.markdown(prompt)
                st.session_state.history_user.append(prompt)

            with st.chat_message("assistant"):
                message_placeholder = st.empty()
                message_placeholder.markdown("Analyzing...")
                try:
                    full_resp = query_model(prompt, st.session_state.file_content)
                    message_placeholder.markdown(full_resp)
                    st.session_state.history_answer.append(full_resp)

                except genai.types.generation_types.BlockedPromptException as e:
                    st.exception(e)
                except Exception as e:
                    st.exception(e)
            with st.sidebar:
                # Download button for conversation history
                history_text = generate_conversation_history()
                st.download_button(label="Download History",
                        data=history_text,
                        file_name="conversation_history.md",
                        mime="text/markdown")
                    
        
    
def query_model(query, context):
    instructions = f"Compte tenu du contexte du journal, répondez à la question: {query}"
    template = f"Instruction:\n{instructions}\nINPUTDATA:\n{context}\nResponse:\n"
    pipe = pipeline(
        "text-generation",
        model=model,
        tokenizer=tokenizer,
    )

    generation_args = {
        "max_new_tokens": 500,
        "return_full_text": False,
        "temperature": 0.0,
        "do_sample": False,
    }
    messages = [
    {"role": "user", "content": template}
    ]

    output = pipe(messages, **generation_args)
    response = output[0]['generated_text']
    return response


def display_conversation():
    if 'history_user' in st.session_state and 'history_answer' in st.session_state:
        n = min(len(st.session_state.history_user), len(st.session_state.history_answer))
        for i in range(n):
            with st.container():
                with st.chat_message("user"):
                    st.markdown(st.session_state.history_user[i])
                with st.chat_message("assistant"):
                    st.markdown(st.session_state.history_answer[i])

def generate_conversation_history():
    if 'history_user' in st.session_state and 'history_answer' in st.session_state:
        history_content = []
        n = min(len(st.session_state.history_user), len(st.session_state.history_answer))
        for i in range(n):
            user_message = f"User: {st.session_state.history_user[i]}"
            assistant_message = f"Assistant: {st.session_state.history_answer[i]}"
            history_content.append(user_message)
            history_content.append(assistant_message)
        return "\n".join(history_content)
    return "No conversation history available."

if __name__ == "__main__":
    main()
