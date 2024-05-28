import streamlit as st
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import markdown

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

st.set_page_config(page_title="Anomaly detection", page_icon="👻")

def main():
    logo = "bcp_logo.png"
    #st.sidebar.image(logo)
    #st.sidebar.title("Navigation")
    page = st.sidebar.selectbox("Choose a page", ["Anomaly detection"])

    if page == "Anomaly detection":
        show_summary_page()
    
    with st.sidebar:
        if st.button("Clear chat window", use_container_width=True):
            st.session_state.history = []
            st.rerun()

def show_summary_page():
    st.title('Anomaly Detection')
    st.caption('Upload a data quality report log file and detect anomalies within it.')

    file = st.file_uploader("Upload a log file to summarize", type=["log", "txt"])
    #log_entries = st.text_area("Or paste your log entries here:")
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
    if st.button("Detect Anomalies"):
        data_to_summarize = file.getvalue().decode("ISO-8859-1")
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
                    Analyse de la qualité des métadonnées et détection des anomalies pour les tables de la base de données

                    I. Instructions :

                    1. Pour chaque table de la base de données, vérifiez que tous les critères suivants sont respectés :
                    - Le nom de la table respecte le modèle regex : ^[a-zA-Z_][a-zA-Z0-9_]*$.
                    - Total Size et Raw Data Size pour chaque table doivent être des valeurs positives.
                    - Vérifier la corrélation entre Total Size et Raw Data Size pour chaque table, si la corrélation est fortement corrélées donc pas d'anomaly, sinon anomaly (ce point est important)
                    - Number of Rows et Number of Files pour chaque table doivent être positifs.
                    - Chaque attribut de la table doit avoir un nom de classification (ex : Secret, Restreint, Confidentiel).

                    2. Si l'un de ces critères n'est pas respecté, la table doit être marquée comme ayant une anomalie.

                    II. Déroulement de l'analyse :

                    - Examinez les informations suivantes pour chaque table et déterminez si une anomalie est présente.

                    III. Rapport d'anomalie :

                    - Indiquez pour chaque table si une anomalie a été détectée et décrivez la nature de l'anomalie.
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

def query_model(query, context):
    instructions = f"Compte tenu du contexte du journal, répondez à la question: {query}"
    template = f"Instruction:\n{instructions}\nContext:\n{context}\nResponse:\n"
    response = model.generate_content(template)
    return response.text


def display_conversation():
    if 'history_user' in st.session_state and 'history_answer' in st.session_state:
        n = min(len(st.session_state.history_user), len(st.session_state.history_answer))
        for i in range(n):
            with st.container():
                with st.chat_message("user"):
                    st.markdown(st.session_state.history_user[i])
                with st.chat_message("assistant"):
                    st.markdown(st.session_state.history_answer[i])

# def generate_conversation_history():
#     if 'history_user' in st.session_state and 'history_answer' in st.session_state:
#         history_content = []
#         n = min(len(st.session_state.history_user), len(st.session_state.history_answer))
#         for i in range(n):
#             user_message = f"User: {st.session_state.history_user[i]}"
#             assistant_message = f"Assistant: {st.session_state.history_answer[i]}"
#             history_content.append(user_message)
#             history_content.append(assistant_message)
#         return "\n".join(history_content)
#     return "No conversation history available."

if __name__ == "__main__":
    main()
