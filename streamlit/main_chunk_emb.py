import streamlit as st
import numpy as np
import markdown

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import google.generativeai as genai
from transformers import BertModel, BertTokenizer


from dotenv import load_dotenv
import re
from collections import defaultdict

from collections import defaultdict
import re
import torch
from pymilvus import connections, FieldSchema, CollectionSchema, DataType, Collection, utility
# Set up and configure the model globally to avoid reconfiguration

load_dotenv()
#AIzaSyASxqvQhcv04ggzKRVbt7Q8-RpTpxSE0QI | selmaadoudi
#AIzaSyC6EpDYb85c7IqxdLkIu5xGn8CrKadc08A | elmaadoudimohamed1
genai.configure(api_key="AIzaSyASxqvQhcv04ggzKRVbt7Q8-RpTpxSE0QI")
model = genai.GenerativeModel(
    model_name="gemini-1.5-pro-latest",
    generation_config={
        "temperature": 0.01,
        "top_p": 0.75,
        "max_output_tokens": 10000
    },
    safety_settings=[{"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"}]  # Simplified for example
)

# Load pre-trained model and tokenizer
model_name_bert = 'bert-base-uncased'
tokenizer_bert = BertTokenizer.from_pretrained(model_name_bert)
model_bert = BertModel.from_pretrained(model_name_bert)

st.set_page_config(page_title="e-report", page_icon="🤖")

#------------------------------------------------------ Milvus ------------------------------------------------------
# Connect to Milvus
connections.connect("default", host="localhost", port="19530")

# Define your collection schema
fields = [
    FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
    FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=768),
    FieldSchema(name="chunk", dtype=DataType.VARCHAR, max_length=1024)
]
schema = CollectionSchema(fields, "Log embeddings collection")

# Create a collection if it does not exist
collection_name = "log_embeddings"
if collection_name not in utility.list_collections():
    collection = Collection(collection_name, schema)
else:
    collection = Collection(collection_name)

#------------------------------------------------------ Milvus ------------------------------------------------------


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
    response = model.generate_content(template)
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
    
    # Handle file upload
    uploaded_file = st.file_uploader("Upload a log file", type=["log", "txt"])
    if uploaded_file is not None:
        file_content = uploaded_file.read().decode("utf-8")
        st.session_state.file_content = file_content
        #st.session_state.history = []
        process_logs(uploaded_file)
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
                    full_resp = get_answer(prompt)
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
                    
        
    
# def query_model(query, context):
#     instructions = f"Compte tenu du contexte du journal, répondez à la question: {query}"
#     template = f"Instruction:\n{instructions}\nContext:\n{context}\nResponse:\n"
#     response = model.generate_content(template)
#     return response.text

def get_answer(query):
    # Step 1: Generate embedding for the query
    inputs = tokenizer_bert(query, truncation=True, padding=True, return_tensors="pt", max_length=512)
    with torch.no_grad():
        outputs = model_bert(**inputs)
    embedding = outputs.last_hidden_state[:, 0, :].squeeze().numpy().astype(np.float32).tolist()
    
    # Debug: Print the embedding type and shape
    print(f"Embedding type: {type(embedding)}, Embedding length: {len(embedding)}")
    
    # Step 2: Perform similarity check in Milvus
    search_params = {"metric_type": "L2", "params": {"nprobe": 10}}
    results = collection.search(
        data=[embedding],
        anns_field='embedding',
        param=search_params,
        limit=5,
        expr=None
    )
    
    # Step 3: Extract the most similar document texts
    document_texts = []
    if results:
        for result in results[0]:
            doc_id = result.id
            similarity_score = result.distance
            document_text = get_document_text_by_id(doc_id)
            document_texts.append(document_text)
    else:
        return "No similar documents found."
    
    # Combine the texts of the top 5 similar documents
    combined_text = "\n".join(document_texts)
    
    # Step 4: Use the generative model to generate an answer
    template = f"Instruction:\n{query}\nINPUTDATA:{combined_text}\nResponse:\n"
    response = model.generate_content(template)
    
    # Extract the answer from the response
    answer = response.text
    
    return answer

# Function to retrieve document text by ID from the Milvus collection
def get_document_text_by_id(doc_id):
    # Query Milvus to get the document text by ID
    collection.load()
    expr = f"id == {doc_id}"
    results = collection.query(expr=expr, output_fields=["chunk"])
    if results:
        return results[0]["chunk"]
    return "No document found for the given ID."

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

#####################################################


# Function to chunk logs by level
def chunk_logs_by_level(file_content):
    log_levels = defaultdict(list)
    try:
        lines = file_content.split('\n')
        line_count = 0
        for line in lines:
            line_count += 1
            match = re.search(r'\s-\s(INFO|WARNING|ERROR)\s-', line)
            if match:
                log_level = match.group(1)
                log_levels[log_level].append(line)
        if line_count == 0:
            print("The file is empty.")
        elif len(log_levels) == 0:
            print("No log levels matched. Please check the regex and log format.")
    except Exception as e:
        print(f"An error occurred: {e}")
    return log_levels

# Function to generate embedding for a given text group
def get_embedding_for_group(text):
    inputs = tokenizer_bert(text, truncation=True, padding=True, return_tensors="pt", max_length=512)
    with torch.no_grad():
        outputs = model(**inputs)
    embedding = outputs.last_hidden_state[:, 0, :].squeeze().numpy().astype(np.float32).tolist()
    return embedding

# Function to save embeddings and text to Milvus
def save_embeddings_to_milvus(embedding, text, id_counter):
    data = [
        [id_counter, embedding, text]
    ]
    mr = collection.insert(data)
    return mr

# Function to process log files
def process_logs(file_content):
    log_levels = chunk_logs_by_level(file_content)
    try:
        id_counter = 1  # Initialize a counter for manual ID generation
        for level, lines in log_levels.items():
            if lines:
                st.write(f"Processing {level} logs...")
                text = " ".join(lines).strip()
                embedding = get_embedding_for_group(text)
                st.write(f"Generated embedding for {level} level with length: {len(embedding)}")
                mr = save_embeddings_to_milvus(embedding, text, id_counter)
                st.write(f"Insert result: {mr.insert_count}")
                id_counter += 1  # Increment the ID counter

    except Exception as e:
        st.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()