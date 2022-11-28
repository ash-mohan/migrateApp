import logging
import azure.functions as func
import psycopg2
import os
from datetime import datetime
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

def main(msg: func.ServiceBusMessage):

    notification_id = int(msg.get_body().decode('utf-8'))
    logging.info('Python ServiceBus queue trigger processed message: %s',notification_id)

    # TODO: Get connection to database

    # conn = psycopg2.connect(
    #     host="techconfdbserver.postgres.database.azure.com",
    #     database="techconfdb",
    #     user="azureuser",
    #     password="Udacityproject1")
    conn = psycopg2.conn = psycopg2.connect(
        dbname="techconfdb", user="azureuser@techconfdbserver", password="Udacityproject1", host="techconfdbserver.postgres.database.azure.com")
    cur = conn.cursor()
    try:
        # TODO: Get notification message and subject fron database using the notification_id
        cur.execute(f"SELECT message, subject FROM notification where id = {notification_id};")
        notification = cur.fetchone()
        logging.info("Retrieved notification: {notification}")


        # TODO: Get attendees email and name
        cur.execute("SELECT email, first_name, last_name FROM attendee;")
        rows = cur.fetchall()
        logging.info(f"Retrieved attendees: {rows}")

        # TODO: Loop thru each attendee and send an email with a personalized subject
        for row in rows:
            subject = f"Hello {row[1]}: {notification[1]}"
            #sendgrid(row[0], subject, notification[0])
        logging.info("Emails sent to all attendees")

        # TODO: Update the notification table by setting the completed date and updating the status with the total number of attendees notified
        status = f'Notified {len(rows)} attendees'
        sql = """ UPDATE notification
                SET status = %s,
                completed_date = %s
                WHERE id = %s"""
        cur.execute(sql,(status, datetime.utcnow(), notification_id))
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
    finally:
        logging.info('closing connection')
        # TODO: Close connection
        cur.close()
        conn.close()

def sendgrid(email, subject, body):
    message = Mail(
        from_email='info@techconf.com',
        to_emails=email,
        subject=subject,
        plain_text_content=body)
    SENDGRID_API_KEY = ''
    sg = SendGridAPIClient(SENDGRID_API_KEY)    
    sg.send(message)