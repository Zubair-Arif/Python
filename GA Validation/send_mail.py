from smtplib import SMTP
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import date

def send_email(data_df,log_list,from_email,to_email,cc_email,email_pass,log_link):
    
    """ sending logs to gmail """
    
    message = MIMEMultipart()
    message['Subject'] = str(date.today()) + " SRMG validation daily Report"
    message['From'] = from_email
    message['To'] = ", ".join(to_email)
    message['Cc'] = ", ".join(cc_email)

    for i, l in zip(data_df,log_list):
        log = "\n".join(l)
        part1 = MIMEText(log , "plain")
        message.attach(part1)
        part2 = MIMEText(i, "html")
        message.attach(part2)
    log_list_2 = []
    log = "\nFor detailed view of the logs click the below link and select the recent log"
    log_list_2.append(log)
    log_list_2.append(log_link)
    log = "\n".join(log_list_2)
    part3 = MIMEText(log , "plain")
    message.attach(part3)
    msg_body = message.as_string()
    try :
        server = SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(message['From'], email_pass)
        server.sendmail(message['From'],(to_email+cc_email), msg_body)
        server.quit()
        print("mail_sent")
    except Exception as e:
         print("mail not sent",e)