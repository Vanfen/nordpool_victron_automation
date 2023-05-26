import imaplib
import email
import os

from dotenv import load_dotenv
from send_notification import send_price_update_notification_via_email

load_dotenv()

port = os.getenv("MAIL_PORT")  # For SSL
smtp_server = os.getenv("MAIL_HOST")
username = os.getenv("MAIL_UNAME")
mail_password = os.getenv('MAIL_PASSWORD')

mail_from = os.getenv('MAIL_FROM')

mail = imaplib.IMAP4_SSL(smtp_server)
mail.login(username, mail_password)
mail.select("Inbox")
_, search_data = mail.search(None, f'UNSEEN FROM "{mail_from}"')
my_messages = []
was_updated = False

def floatCheck(number):
	try:
		float(number)
	except ValueError:
		return 0
	return 1

def read_inbox():
    if search_data[0]:	
        for num in search_data[0].split():
            email_data = {}
            _, data = mail.fetch(num, '(RFC822)')
            _, b = data[0]
            email_message = email.message_from_bytes(b)
        
            for header in ['subject', 'from']:
                email_data[header] = email_message[header]
            for part in email_message.walk():
                if part.get_content_type() == "text/plain":
                    body = part.get_payload(decode=True)
                    email_data['body'] = body.decode()
                elif part.get_content_type() == "text/html":
                    html_body = part.get_payload(decode=True)
            my_messages.append(email_data)
        price_index = my_messages[-1]['body'].find("Price:")
        subject_check = my_messages[-1]['subject'].find("Price setup")
        if ((price_index != -1) & (subject_check != -1)):
            msg_body = my_messages[-1]['body']
            new_price = msg_body[price_index+7:]
            file = open("./price_to_compare.txt", 'r')
            old_price = file.readline()
            file.close()
            new_price_check = floatCheck(new_price)
            old_price_check = floatCheck(old_price)
            if (new_price_check == old_price_check == 1):
                if (float(new_price) != float(old_price)):  
                    file = open("./price_to_compare.txt", 'w')
                    file.write(new_price)
                    file.close()
                    was_updated = True
            old_price = old_price.strip()
            new_price = new_price.strip()
            print("new price {} old price {}".format(new_price, old_price))
        
        send_price_update_notification_via_email(was_updated=was_updated, old_price=old_price, new_price=new_price)
    else:
        print('no emails found')