import datetime
import smtplib, ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import os
import ssl
from dotenv import load_dotenv

load_dotenv()

port = os.getenv("MAIL_PORT")  # For SSL
smtp_server = os.getenv("MAIL_HOST")
sender_email = os.getenv("MAIL_UNAME")
mail_password = os.getenv('MAIL_PASSWORD')
receipients = ['email1@gmail.com', ...]

def send_price_update_notification_via_email (was_updated: bool, old_price: str, new_price: str):
    receiver_email = receipients
    body = str(old_price) + " to " + str(new_price)
    if (was_updated):
        message = f"""\
            Subject: Nordpool Data
            To: {receiver_email}
            From: {sender_email}
            Price for VictronEnergy scheduler script updated \n from """ + body.strip()
    else:
        message = f"""\
            Subject: Nordpool Data
            To: {receiver_email}
            From: {sender_email}
            Price for VictronEnergy scheduler was not updated. You tried to update \n""" + body.strip()
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
            server.login(sender_email, mail_password)
            server.sendmail(sender_email, receiver_email, message)

def send_schedule_update_notification_via_email(parsed_data, country):
    print("Send email notification")

    # Define the css for html
    css = '''
    <style>
    #schedules {
    font-family: Arial, Helvetica, sans-serif;
    border-collapse: collapse;
    width: 100%;
    }

    #schedules td, #schedules th {
    border: 1px solid #ddd;
    padding: 8px;
    }

    #even {background: #f2f2f2;}

    #schedules tr:hover {background: #ddd;}

    #schedules th {
    padding-top: 12px;
    padding-bottom: 12px;
    text-align: left;
    background: #04AA6D;
    color: white;
    }
    </style>'''
    # Define the HTML document
    html_begin = f'''
        <html>
            <head>
            {css}
            </head>
            <body>
                <h1>Nord-Pool day ahead prices update for {country}.</h1>
                <p>Min Price: {parsed_data[country]['Min']}</p>
                <p>Max Price: {parsed_data[country]['Max']}</p>
                <p>Average Price: {parsed_data[country]['Average']}</p>
                <p>Update time: {parsed_data[country]['NordPool_Update_Time']}</p>
                <p>Compare to price: {parsed_data[country]['Compare_To']}</p>
                <table id="schedules">
                    <thead>
                        <tr><th>Price</th><th>Start Time</th><th>End Time</th></tr>
                    </thead>
                    <tbody>'''
    html_schedules = ""
    i = 1
    for schedule in parsed_data['LV']['Values']:
        if i % 2 == 0:
            set_even_id = 'id="even"'
        else:
            set_even_id = ''
        html_schedules += (f"<tr {set_even_id}><td>{str(schedule['Price'])}</td><td>{schedule['Start_Datetime']}</td><td>{schedule['End_Datetime']}</td></tr>")
        i += 1
    html_end = '''
                    </tbody>
                </table>
            </body>
        </html>'''
    
    html = html_begin + html_schedules + html_end

    # Set up the email addresses and password. Please replace below with your email address and password
    email_from = sender_email
    password = mail_password
    email_to = receipients

    # Generate today's date to be included in the email Subject
    date_str = datetime.datetime.today().strftime('%Y-%m-%d')

    # Create a MIMEMultipart class, and set up the From, To, Subject fields
    email_message = MIMEMultipart()
    email_message['From'] = email_from
    email_message['To'] = ''.join(email_to)
    email_message['Subject'] = f'Nord-Pool - Victron report - {date_str}'

    # Attach the html doc defined earlier, as a MIMEText html content type to the MIME message
    email_message.attach(MIMEText(html, "html"))
    # Convert it as a string
    email_string = email_message.as_string()

    # Connect to the Gmail SMTP server and Send Email
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
        server.login(email_from, password)
        server.sendmail(email_from, email_to, email_string)