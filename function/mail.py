import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

sender_email = "iotserver@deltadigitalid.com"#"kaze.service8@gmail.com"
password = "iotserver123"#"K@ze1234"

def send(receiver_email,subject,content):
	message = MIMEMultipart("alternative")
	message["Subject"] = subject
	message["From"] = sender_email
	message["To"] = receiver_email

	part = MIMEText(content, "html")
	message.attach(part)

	context = ssl.create_default_context()
	# with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
	with smtplib.SMTP_SSL("iix17.cloudhost.id", 465, context=context) as server:
		server.login(sender_email, password)
		server.sendmail(
			sender_email, receiver_email, message.as_string()
		)
    