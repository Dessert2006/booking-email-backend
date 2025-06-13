from flask import Flask, request, jsonify
from flask_cors import CORS
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import os
from dotenv import load_dotenv
import schedule
import time
import threading
import pandas as pd
from datetime import datetime
from firebase_admin import credentials, firestore, initialize_app

# Load environment variables
load_dotenv()

# Initialize Flask app
app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": ["http://localhost:3000", "https://booking-report.vercel.app"]}})

# Initialize Firestore with the specified path to credentials file
credential_path = os.path.join(os.path.dirname(__file__), "firebase-admin-sdk.json")

# Verify that the credentials file exists
if not os.path.exists(credential_path):
    raise FileNotFoundError(f"Firebase credentials file not found at: {credential_path}")

cred = credentials.Certificate(credential_path)
initialize_app(cred)
db = firestore.client()

# Email configuration
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SENDER_EMAIL_MUMBAI = "tanks@dessertmarine.com"
SENDER_PASSWORD_MUMBAI = "awou kyet gbgs btud"
# SENDER_EMAIL_GUJARAT = "mundra@dessertmarine.com"
# SENDER_PASSWORD_GUJARAT = "<add-mundra-password-here>"

# Helper to get sender email/password by location
BRANCH_EMAILS = {
    "MUMBAI": (SENDER_EMAIL_MUMBAI, SENDER_PASSWORD_MUMBAI),
    # "GUJARAT": (SENDER_EMAIL_GUJARAT, SENDER_PASSWORD_GUJARAT)
}

def get_sender_by_location(location):
    if location:
        loc = location.strip().upper()
        if "MUMBAI" in loc:
            return SENDER_EMAIL_MUMBAI, SENDER_PASSWORD_MUMBAI
        # if "GUJARAT" in loc or "MUNDRA" in loc:
        #     return SENDER_EMAIL_GUJARAT, SENDER_PASSWORD_GUJARAT
    return SENDER_EMAIL_MUMBAI, SENDER_PASSWORD_MUMBAI

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
        
        # Choose sender email/password by location
        sender_email, sender_password = get_sender_by_location(data.get('location', ''))
        msg['From'] = sender_email
        msg['To'] = customer_email
        msg['Cc'] = sales_person_email
        msg['Subject'] = f"{customer_name} | SHIPPED ON BOARD | {vessel} | {booking_no} | {bl_no}"

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
        print(f"Attempting to send email from {sender_email} to {customer_email} with CC {sales_person_email}")

        # Connect to SMTP server
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            print("TLS started")
            server.login(sender_email, sender_password)
            print("Login successful")
            server.sendmail(sender_email, [customer_email, sales_person_email], msg.as_string())
            print("Email sent successfully")

        return jsonify({"message": "Email sent successfully"}), 200

    except Exception as e:
        print(f"Error sending email: {str(e)}")
        return jsonify({"error": str(e)}), 500

def fetch_bookings_by_salesperson():
    """
    Fetch all bookings from Firestore and group them by salesperson.
    Returns a dictionary with salesperson emails as keys and lists of bookings as values.
    """
    try:
        bookings_ref = db.collection("entries")
        docs = bookings_ref.stream()

        # Dictionary to store bookings by salesperson email
        bookings_by_salesperson = {}

        for doc in docs:
            entry = doc.to_dict()
            entry["id"] = doc.id

            # Debug: Print the entire entry to inspect its structure
            print(f"Entry {entry['id']}: {entry}")

            # Extract customer field
            customer = entry.get("customer", {})
            
            # Check if customer is a dictionary
            if not isinstance(customer, dict):
                print(f"Skipping entry {entry['id']}: 'customer' field is not a dictionary, found {type(customer)}: {customer}")
                continue

            # Extract salesperson email(s) from the customer map
            sales_person_emails = customer.get("salesPersonEmail", [])

            if not sales_person_emails:
                print(f"No salesperson email found for entry {entry['id']}")
                continue

            # Extract container number from equipmentDetails or directly
            container_no = ""
            if "equipmentDetails" in entry and entry["equipmentDetails"]:
                # Ensure equipmentDetails is a list
                if isinstance(entry["equipmentDetails"], list):
                    container_no = ", ".join(
                        eq["containerNo"] for eq in entry["equipmentDetails"] if eq.get("containerNo")
                    )
                else:
                    print(f"Skipping entry {entry['id']}: 'equipmentDetails' is not a list, found {type(entry['equipmentDetails'])}")
            else:
                container_no = entry.get("containerNo", "")

            # Prepare booking data
            booking_data = {
                "Customer Name": customer.get("name", ""),
                "Sales Person": customer.get("salesPerson", ""),
                "Booking No": entry.get("bookingNo", ""),
                "SOB Date": entry.get("sobDate", ""),
                "Vessel": entry.get("vessel", ""),
                "Voyage": entry.get("voyage", ""),
                "POL": entry.get("pol", ""),
                "POD": entry.get("pod", ""),
                "FPOD": entry.get("fpod", ""),
                "Container No": container_no,
                "Volume": entry.get("volume", ""),
                "BL No": entry.get("blNo", ""),
                "Booking Date": entry.get("bookingDate", ""),
                "ETD": entry.get("etd", ""),
                "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }

            # Add pending SI and pending BL fields
            booking_data["Pending SI"] = "Yes" if not entry.get("siFiled", False) else "No"
            booking_data["Pending BL"] = "Yes" if not entry.get("blReleased", False) else "No"

            # Add location to booking data
            booking_data["Location"] = entry.get("location", "")

            # Add booking to each salesperson's list, grouped by location
            for email in sales_person_emails:
                if email:
                    if email not in bookings_by_salesperson:
                        bookings_by_salesperson[email] = {}
                    loc = booking_data["Location"]
                    if loc not in bookings_by_salesperson[email]:
                        bookings_by_salesperson[email][loc] = []
                    bookings_by_salesperson[email][loc].append(booking_data)

        return bookings_by_salesperson

    except Exception as e:
        print(f"Error fetching bookings from Firestore: {str(e)}")
        return {}

def generate_excel_report(salesperson_email, bookings):
    """
    Generate an Excel report for a salesperson's bookings.
    Returns the filename of the generated Excel file.
    """
    if not bookings:
        print(f"No bookings found for salesperson {salesperson_email}")
        return None

    # Create a DataFrame
    df = pd.DataFrame(bookings)
    # Sort first by Customer Name (alphabetically), then by ETD (earliest first)
    if not df.empty:
        df = df.sort_values(by=["Customer Name", "ETD"], ascending=[True, True], key=lambda col: col.str.lower() if col.dtype == object else col)
    # Format date columns to dd-mm-yyyy
    date_columns = [col for col in df.columns if 'date' in col.lower() or col.upper() == 'ETD']
    for col in date_columns:
        df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%d-%m-%Y')
    # Generate Excel file
    excel_filename = f"booking_report_{salesperson_email.split('@')[0]}_{datetime.now().strftime('%Y-%m-%d')}.xlsx"
    df.to_excel(excel_filename, index=False)
    return excel_filename

def send_daily_report():
    """
    Send a daily report to each salesperson with their bookings, grouped by location.
    """
    try:
        bookings_by_salesperson = fetch_bookings_by_salesperson()
        if not bookings_by_salesperson:
            print("No bookings found for any salesperson.")
            return
        for salesperson_email, loc_dict in bookings_by_salesperson.items():
            # Combine all bookings for this salesperson across all locations
            all_bookings = []
            for location, bookings in loc_dict.items():
                all_bookings.extend(bookings)
            if not all_bookings:
                continue
            print(f"Generating report for {salesperson_email} with {len(all_bookings)} bookings (all locations)")
            excel_file = generate_excel_report(salesperson_email, all_bookings)
            if not excel_file:
                continue
            # Use Mumbai sender for now (or pick by first booking location if needed)
            sender_email, sender_password = get_sender_by_location(all_bookings[0].get('Location', ''))
            msg = MIMEMultipart()
            msg['From'] = sender_email
            msg['Subject'] = f"Daily Booking Report - {datetime.now().strftime('%Y-%m-%d')}"

            # Parse all sales person emails (comma-separated or list)
            all_sales_emails = []
            if isinstance(salesperson_email, str):
                all_sales_emails = [e.strip() for e in salesperson_email.split(',') if e.strip()]
            elif isinstance(salesperson_email, list):
                for e in salesperson_email:
                    all_sales_emails.extend([x.strip() for x in str(e).split(',') if x.strip()])
            else:
                all_sales_emails = [str(salesperson_email)]
            msg['To'] = ', '.join(all_sales_emails)
            sales_person_name = all_bookings[0].get('Sales Person', 'Salesperson') if all_bookings else 'Salesperson'
            body = f"""
Dear {sales_person_name},

Please find attached the daily booking report as of {datetime.now().strftime('%Y-%m-%d')} (includes all locations).

For any queries, please write to the CS team.

Note: This is an Auto Generated Mail.
"""
            msg.attach(MIMEText(body, 'plain'))
            with open(excel_file, 'rb') as f:
                attachment = MIMEApplication(f.read(), _subtype="xlsx")
                attachment.add_header('Content-Disposition', 'attachment', filename=excel_file)
                msg.attach(attachment)
            with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
                server.starttls()
                server.login(sender_email, sender_password)
                server.sendmail(sender_email, all_sales_emails, msg.as_string())
                print(f"Daily report sent to {salesperson_email} (all locations)")
            os.remove(excel_file)
    except Exception as e:
        print(f"Error sending daily reports: {str(e)}")

# Schedule the daily report at 8 PM IST
# Current time is 11:00 AM IST on June 13, 2025, so the report will run at 8 PM today
schedule.every().day.at("17:21").do(send_daily_report)

# Function to run the scheduler in a separate thread
def run_scheduler():
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute

if __name__ == '__main__':
    print(f"Starting server with SENDER_EMAIL_MUMBAI: {SENDER_EMAIL_MUMBAI}")
    
    # Start the scheduler in a separate thread
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    
    # Start the Flask app
    app.run(host='0.0.0.0', port=5000, debug=True)