from flask import Flask, request, jsonify
from flask_cors import CORS
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": ["http://localhost:3000", "https://booking-report.vercel.app"]}})  # Allow React app

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SENDER_EMAIL = "tanks@dessertmarine.com"  # Replace with your actual Gmail address
SENDER_PASSWORD = "awou kyet gbgs btud"  # Set to your App Password

@app.route('/api/send-sob-email', methods=['POST'])
def send_sob_email():
    try:
        # Log the incoming request data
        data = request.get_json()
        print(f"Received data: {data}")

        # Extract email data
        customer_email = data.get('customer_email')
        sales_person_email = data.get('sales_person_email')
        customer_name = data.get('customer_name')
        booking_no = data.get('booking_no')
        sob_date = data.get('sob_date')
        vessel = data.get('vessel')
        voyage = data.get('voyage')
        pol = data.get('pol')
        pod = data.get('pod')
        fpod = data.get('fpod', '')
        container_no = data.get('container_no')
        volume = data.get('volume')
        bl_no = data.get('bl_no', '')

        # Handle case where emails might be lists
        if isinstance(customer_email, list):
            customer_email = customer_email[0] if customer_email else None
        if isinstance(sales_person_email, list):
            sales_person_email = sales_person_email[0] if sales_person_email else None

        if not customer_email or not sales_person_email:
            print("Missing customer_email or sales_person_email")
            return jsonify({"error": "Customer or salesperson email missing"}), 400

        # Handle container numbers with detailed logging
        print(f"Raw container_no: {container_no} (type: {type(container_no)})")
        if container_no is None:
            container_no_str = ""
        elif isinstance(container_no, list):
            container_no_str = ", ".join(str(c) for c in container_no) if container_no else ""
        elif isinstance(container_no, str):
            container_no_str = container_no
        else:
            container_no_str = str(container_no)
        print(f"Processed container_no_str: {container_no_str}")

        # Create email
        msg = MIMEMultipart('alternative')
        msg['From'] = SENDER_EMAIL
        msg['To'] = customer_email
        msg['Cc'] = sales_person_email
        msg['Subject'] = f"SHIPPED ON BOARD | {vessel} | {booking_no} | {bl_no}"

        # Plain text version (fallback)
        plain_body = f"""
Dear Sir/Madam,

We are pleased to confirm your Subject Shipment is Shipped On Board.
Details as Below:

BOOKING NO: {booking_no}
POL: {pol}
POD: {pod}
FPOD: {fpod}
VOLUME: {volume}
CONTAINER NO: {container_no_str}
VESSEL: {vessel}
VOYAGE: {voyage}
SOB DATE: {sob_date}

For any queries please write to cs team.

Note: This is an Auto Generated Mail.
"""
        msg.attach(MIMEText(plain_body, 'plain'))

        # HTML version with table
        html_body = f"""
<html>
<body style="font-family: Arial, sans-serif;">
    <p>Dear Sir/Madam,</p>
    <p>We are pleased to confirm your Subject Shipment is Shipped On Board.</p>
    <p>Details as Below:</p>
    <table border="1" cellpadding="5" cellspacing="0" style="border-collapse: collapse;">
        <tr style="background-color: #f2f2f2;">
            <th>BOOKING NO</th>
            <th>POL</th>
            <th>POD</th>
            <th>FPOD</th>
            <th>VOLUME</th>
            <th>CONTAINER NO</th>
            <th>VESSEL</th>
            <th>VOYAGE</th>
            <th>SOB DATE</th>
        </tr>
        <tr>
            <td>{booking_no}</td>
            <td>{pol}</td>
            <td>{pod}</td>
            <td>{fpod}</td>
            <td>{volume}</td>
            <td>{container_no_str if container_no_str else 'N/A'}</td>
            <td>{vessel}</td>
            <td>{voyage}</td>
            <td>{sob_date}</td>
        </tr>
    </table>
    <p>For any queries please write to cs team.</p>
    <p><em>Note: This is an Auto Generated Mail.</em></p>
</body>
</html>
"""
        msg.attach(MIMEText(html_body, 'html'))

        # Log before sending email
        print(f"Attempting to send email from {SENDER_EMAIL} to {customer_email} with CC {sales_person_email}")

        # Connect to SMTP server
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            print("TLS started")
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            print("Login successful")
            server.sendmail(SENDER_EMAIL, [customer_email, sales_person_email], msg.as_string())
            print("Email sent successfully")

        return jsonify({"message": "Email sent successfully"}), 200

    except Exception as e:
        print(f"Error sending email: {str(e)}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    print(f"Starting server with SENDER_EMAIL: {SENDER_EMAIL}, SENDER_PASSWORD: {SENDER_PASSWORD}")
    app.run(host='0.0.0.0', port=5000, debug=True)