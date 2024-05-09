import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime
import re

email = "elmaadoudimohamed1@gmail.com"
password = "wrobezkyasmarbet"
receiver_email = "hichamtejda@gmail.com"
subject = 'Metadata Quality Report Warnings'
smtp_server = "smtp.gmail.com"
smtp_port = 587
output_file = 'metadata_quality_summary.txt'

with open('metadata_quality_report.log', 'r') as log_file:
    log_contents = log_file.readlines()

start_datetime = datetime.strptime(log_contents[0].split(',')[0], '%Y-%m-%d %H:%M:%S')
end_datetime = datetime.strptime(log_contents[-1].split(',')[0], '%Y-%m-%d %H:%M:%S')

warnings_summary = {}
errors_summary = {}

for line in log_contents:
    if 'WARNING' in line or 'ERROR' in line:
        match = re.search(r'(?:table|Table)[:\s]+\'?(\w+)\'?', line)
        if match:
            table_name = match.group(1)
            if 'WARNING' in line:
                warnings_summary[table_name] = warnings_summary.get(table_name, 0) + 1
            if 'ERROR' in line:
                errors_summary[table_name] = errors_summary.get(table_name, 0) + 1

warnings_summary_html = '<table border="1"><tr><th>Table</th><th>Warnings</th></tr>'
for table, count in warnings_summary.items():
    warnings_summary_html += f'<tr><td>{table}</td><td>{count}</td></tr>'
warnings_summary_html += '</table>'

errors_summary_html = '<table border="1"><tr><th>Table</th><th>Errors</th></tr>'
for table, count in errors_summary.items():
    errors_summary_html += f'<tr><td>{table}</td><td>{count}</td></tr>'
errors_summary_html += '</table>'

summary_content = f"""\
<html>
<head>
  <style>
    body {{
      font-family: Arial, sans-serif;
    }}

    table {{
      border-collapse: collapse;
      width: 30%;
    }}

    th, td {{
      border: 1px solid #ddd;
      padding: 8px;
      text-align: left;
    }}

    th {{
      background-color: #f2f2f2;
    }}

    tr:nth-child(even) {{
      background-color: #f9f9f9;
    }}
  </style>
</head>
<body>
<h2>Metadata Quality Report Summary</h2>
<p><strong>Datetime of the start of logs:</strong> {start_datetime}</p>
<p><strong>Datetime of the end of logs:</strong> {end_datetime}</p>

<h3>Warnings Summary:</h3>
<div>
  {warnings_summary_html}
</div>
<h3>Errors Summary:</h3>
<div>
  {errors_summary_html}
</div>
</body>
</html>
"""



message = MIMEMultipart()
message['From'] = email
message['To'] = receiver_email
message['Subject'] = subject

message.attach(MIMEText(summary_content, 'html'))

with open(output_file, 'rb') as attachment:
    part = MIMEBase('application', 'octet-stream')
    part.set_payload(attachment.read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', f"attachment; filename= {output_file}")
    message.attach(part)

try:
    server = smtplib.SMTP(smtp_server, smtp_port)
    server.starttls()
    server.login(email, password)
    server.sendmail(email, receiver_email, message.as_string())
    print("Email sent successfully")
except Exception as e:
    print(f"Failed to send email: {e}")
finally:
    server.quit()