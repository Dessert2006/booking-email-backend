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
from datetime import datetime, timedelta
import pytz
from firebase_admin import credentials, firestore, initialize_app

# Load environment variables
load_dotenv()

# Initialize Flask app
app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": ["http://localhost:3000", "https://booking-report.vercel.app"]}})

# Initialize Firestore
credential_path = os.environ.get(
    "GOOGLE_APPLICATION_CREDENTIALS",
    os.path.join(os.path.dirname(__file__), "firebase-admin-sdk.json"),
)
if not os.path.exists(credential_path):
    raise FileNotFoundError(f"Firebase credentials file not found at: {credential_path}")

cred = credentials.Certificate(credential_path)
initialize_app(cred)
db = firestore.client()

# Email configuration
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587

# You can move these to env vars if you prefer; leaving as-is for drop-in compatibility
SENDER_EMAIL_MUMBAI = "info@dessertmarine.com"
SENDER_PASSWORD_MUMBAI = "gsxb yivs dscy hkrk"      # app password (Gmail shows spaces)
SENDER_EMAIL_GUJARAT = "mundra@dessertmarine.com"
SENDER_PASSWORD_GUJARAT = "aljw cixn pbok lxpk"     # app password (with spaces)

BRANCH_EMAILS = {
    "MUMBAI": (SENDER_EMAIL_MUMBAI, SENDER_PASSWORD_MUMBAI),
    "GUJARAT": (SENDER_EMAIL_GUJARAT, SENDER_PASSWORD_GUJARAT),
}

def normalized_app_password(pw: str) -> str:
    """Gmail app passwords are shown with spaces; SMTP expects no spaces."""
    return pw.replace(" ", "") if isinstance(pw, str) else pw

def get_sender_by_location(location):
    if location:
        loc = str(location).strip().upper()
        if "MUMBAI" in loc:
            return BRANCH_EMAILS["MUMBAI"]
        if "GUJARAT" in loc:
            return BRANCH_EMAILS["GUJARAT"]
    return BRANCH_EMAILS["MUMBAI"]

def parse_si_cutoff_date(si_cutoff):
    """Parse SI cutoff date from dd/mm-hhmm HRS format (e.g., 12/06-1400 HRS) to timezone-aware datetime."""
    try:
        date_part, time_part = si_cutoff.split('-')
        hour_minute = time_part.replace(" HRS", "").strip()
        day, month = date_part.split('/')
        hour, minute = hour_minute[:2], hour_minute[2:]
        year = datetime.now().year
        dt = datetime.strptime(f"{day}/{month}/{year} {hour}:{minute}", "%d/%m/%Y %H:%M")
        ist = pytz.timezone('Asia/Kolkata')
        return ist.localize(dt)
    except Exception as e:
        print(f"Error parsing SI cutoff date {si_cutoff}: {str(e)}")
        return None

def fetch_si_cutoff_data():
    """
    Fetch bookings with SI cutoff dates and group by customer/salesperson.
    Returns a dictionary with customer emails as keys and lists of bookings as values.
    """
    try:
        bookings_ref = db.collection("entries")
        docs = bookings_ref.stream()
        si_cutoff_data = {}

        for doc in docs:
            entry = doc.to_dict()
            entry["id"] = doc.id

            # Skip if SI is already filed
            si_filed = entry.get("siFiled", False)
            if si_filed:
                print(f"SI already filed for entry {entry['id']}, skipping SI cutoff reminder.")
                continue

            si_cutoff = entry.get("siCutOff", "")
            if not si_cutoff:
                print(f"No SI cutoff found for entry {entry['id']}")
                continue

            si_cutoff_dt = parse_si_cutoff_date(si_cutoff)
            if not si_cutoff_dt:
                print(f"Invalid SI cutoff date for entry {entry['id']}: {si_cutoff}")
                continue

            customer = entry.get("customer", {})
            if not isinstance(customer, dict):
                print(f"Skipping entry {entry['id']}: 'customer' field is not a dictionary, found {type(customer)}: {customer}")
                continue

            customer_emails = customer.get("customerEmail", [])
            if not customer_emails:
                print(f"No customer email found for entry {entry['id']}")
                continue
            customer_emails = [email.strip() for email in customer_emails if email.strip()]
            print(f"Fetched customer emails for booking {entry.get('bookingNo', entry['id'])}: {customer_emails}")

            sales_person_emails = customer.get("salesPersonEmail", [])
            if not sales_person_emails:
                print(f"No salesperson email found for entry {entry['id']}")
                continue
            sales_person_emails = [email.strip() for email in sales_person_emails if email.strip()]

            customer_name = customer.get("name", "")
            booking_no = entry.get("bookingNo", "")
            if not booking_no:
                print(f"No booking number found for entry {entry['id']}")
                continue

            volume = entry.get("volume", "")

            # Location can be a string or {name: "..."}
            raw_loc = entry.get("location", "")
            location = raw_loc.get("name") if isinstance(raw_loc, dict) else raw_loc

            reminder_data = {
                "Customer Emails": customer_emails,
                "Sales Person Emails": sales_person_emails,
                "Customer Name": customer_name,
                "Booking No": booking_no,
                "SI Cutoff": si_cutoff_dt,
                "Vessel": entry.get("vessel", ""),
                "Voyage": entry.get("voyage", ""),
                "FPOD": entry.get("fpod", ""),
                "Volume": volume,
                "Location": location or "",
                "POL": entry.get("pol", "")
            }

            # Group by customer emails as a tuple to handle multiple emails
            customer_emails_key = tuple(customer_emails)
            if customer_emails_key not in si_cutoff_data:
                si_cutoff_data[customer_emails_key] = []
            si_cutoff_data[customer_emails_key].append(reminder_data)

        return si_cutoff_data

    except Exception as e:
        print(f"Error fetching SI cutoff data: {str(e)}")
        return {}

def send_si_cutoff_reminder():
    """
    Send SI cutoff reminders 48 and 24 hours before the cutoff date.
    """
    try:
        si_cutoff_data = fetch_si_cutoff_data()
        if not si_cutoff_data:
            print("No SI cutoff data found.")
            return

        now = datetime.now(pytz.timezone('Asia/Kolkata'))
        print(f"Checking SI cutoff reminders at {now}")

        for customer_emails_key, bookings in si_cutoff_data.items():
            customer_emails = list(customer_emails_key)  # Convert tuple back to list
            for booking in bookings:
                si_cutoff = booking["SI Cutoff"]
                time_diff = si_cutoff - now
                hours_diff = time_diff.total_seconds() / 3600

                if hours_diff < 0:
                    print(f"Skipping booking {booking['Booking No']} for {customer_emails}: SI Cutoff at {si_cutoff} has already passed (hours remaining: {hours_diff})")
                    continue

                print(f"Booking {booking['Booking No']} for {customer_emails}: SI Cutoff at {si_cutoff}, hours remaining: {hours_diff}")

                reminder_type = None
                if 47.5 <= hours_diff <= 48.5:
                    reminder_type = "48 hours"
                elif 23.5 <= hours_diff <= 24.5:
                    reminder_type = "24 hours"

                if reminder_type:
                    sender_email, sender_password = get_sender_by_location(booking.get("Location", "MUMBAI"))
                    msg = MIMEMultipart('alternative')
                    msg['From'] = sender_email
                    msg['To'] = ", ".join(customer_emails)
                    msg['Cc'] = ", ".join(booking["Sales Person Emails"])
                    msg['Subject'] = f"!! Reminder for Pending SI !! Booking No: {booking['Booking No']} // Vessel: {booking['Vessel']} // Customer Name: {booking['Customer Name']}"

                    plain_body = f"""
Dear Sir / Madam,

Please note the SI cut-off for below shipment is nearing & request you to please send us the SI on info@dessertmarine.com without delays.

Any change in shipment planning please notify CS team for timely roll-over.

DO NOT REPLY ON THIS MAIL.

Booking No: {booking['Booking No']}
SI Cutoff: {booking['SI Cutoff'].strftime('%d/%m/%Y %H:%M') if booking['SI Cutoff'] else 'N/A'}
Volume: {booking['Volume']}
POL: {booking['POL']}
FPOD: {booking['FPOD']}
Vessel: {booking['Vessel']}
Voyage: {booking['Voyage']}

Note: This is System Generated email. If the SI is already submitted, please ignore & coordinate with doc team for the first print & further process.

Thank you for your support.

Regards,
Dessert Marine Services (I) Pvt Ltd
info@dessertmarine.com
doc@dessertmarine.com
"""
                    msg.attach(MIMEText(plain_body, 'plain'))

                    html_body = f"""
<html>
<body style="font-family: Arial, sans-serif;">
    <p>Dear Sir / Madam,</p>
    <p>Please note the SI cut-off for below shipment is nearing & request you to please send us the SI on <a href="mailto:info@dessertmarine.com">info@dessertmarine.com</a> without delays.</p>
    <p>Any change in shipment planning please notify CS team for timely roll-over.</p>
    <p><strong>DO NOT REPLY ON THIS MAIL.</strong></p>
    <table border="1" cellpadding="5" cellspacing="0" style="border-collapse: collapse;">
        <tr style="background-color: #f2f2f2;">
            <th>Booking No</th>
            <th>SI Cutoff</th>
            <th>Volume</th>
            <th>POL</th>
            <th>FPOD</th>
            <th>Vessel</th>
            <th>Voyage</th>
        </tr>
        <tr>
            <td>{booking['Booking No']}</td>
            <td>{booking['SI Cutoff'].strftime('%d/%m/%Y %H:%M') if booking['SI Cutoff'] else 'N/A'}</td>
            <td>{booking['Volume'] if booking['Volume'] else 'N/A'}</td>
            <td>{booking['POL'] if booking['POL'] else 'N/A'}</td>
            <td>{booking['FPOD']}</td>
            <td>{booking['Vessel']}</td>
            <td>{booking['Voyage']}</td>
        </tr>
    </table>
    <p><em>Note: This is System Generated email. If the SI is already submitted, please ignore & coordinate with doc team for the first print & further process.</em></p>
    <p>Thank you for your support.</p>
    <p>Regards,<br>
    Dessert Marine Services (I) Pvt Ltd<br>
    <a href="mailto:info@dessertmarine.com">info@dessertmarine.com</a><br>
    <a href="mailto:doc@dessertmarine.com">doc@dessertmarine.com</a></p>
</body>
</html>
"""
                    msg.attach(MIMEText(html_body, 'html'))

                    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
                        server.starttls()
                        server.login(sender_email, normalized_app_password(sender_password))
                        recipients = customer_emails + booking["Sales Person Emails"]
                        server.sendmail(sender_email, recipients, msg.as_string())
                        print(f"SI Cutoff reminder ({reminder_type}) sent to {customer_emails} (CC: {booking['Sales Person Emails']}) for booking {booking['Booking No']}")

    except Exception as e:
        print(f"Error sending SI cutoff reminders: {str(e)}")

def fetch_pending_si_data():
    """
    Fetch bookings where SI cutoff is within the next 24 hours from 6:00 PM IST.
    Returns a list of dictionaries with the required fields.
    """
    try:
        bookings_ref = db.collection("entries")
        docs = bookings_ref.stream()
        pending_si_data = []

        now = datetime.now(pytz.timezone('Asia/Kolkata'))
        reference_time = now.replace(hour=18, minute=0, second=0, microsecond=0)
        if now.time() > reference_time.time():
            reference_time = reference_time + timedelta(days=1)

        for doc in docs:
            entry = doc.to_dict()
            entry["id"] = doc.id

            si_cutoff = entry.get("siCutOff", "")
            if not si_cutoff:
                print(f"No SI cutoff found for entry {entry['id']}")
                continue

            si_cutoff_dt = parse_si_cutoff_date(si_cutoff)
            if not si_cutoff_dt:
                print(f"Invalid SI cutoff date for entry {entry['id']}: {si_cutoff}")
                continue

            time_diff = si_cutoff_dt - reference_time
            hours_diff = time_diff.total_seconds() / 3600

            if 0 <= hours_diff <= 24:
                customer = entry.get("customer", {})
                if not isinstance(customer, dict):
                    print(f"Skipping entry {entry['id']}: 'customer' field is not a dictionary, found {type(customer)}: {customer}")
                    continue

                customer_name = customer.get("name", "")
                booking_no = entry.get("bookingNo", "")
                if not booking_no:
                    print(f"No booking number found for entry {entry['id']}")
                    continue

                equipment_type = ""
                if "equipmentDetails" in entry and entry["equipmentDetails"]:
                    if isinstance(entry["equipmentDetails"], list) and len(entry["equipmentDetails"]) > 0:
                        equipment_type = entry["equipmentDetails"][0].get("equipmentType", "")

                etd = entry.get("etd", "")
                if etd:
                    try:
                        etd = pd.to_datetime(etd).strftime('%d-%m-%Y')
                    except Exception as e:
                        print(f"Error parsing ETD for entry {entry['id']}: {e}")
                        etd = ""

                booking_data = {
                    "Booking No": booking_no,
                    "Customer": customer_name,
                    "FPOD": entry.get("fpod", ""),
                    "Equipment Type": equipment_type,
                    "Vessel": entry.get("vessel", ""),
                    "ETD": etd,
                    "SI Cutoff": si_cutoff_dt.strftime('%d/%m/%Y %H:%M')
                }
                pending_si_data.append(booking_data)

        return pending_si_data

    except Exception as e:
        print(f"Error fetching pending SI data: {str(e)}")
        return []

def generate_pending_si_excel(data):
    """
    Generate an Excel file with the pending SI data.
    Returns the filename of the generated Excel file.
    """
    if not data:
        print("No pending SI data to generate Excel report.")
        return None

    df = pd.DataFrame(data)
    if 'SI Cutoff' in df.columns:
        df = df.drop(columns=['SI Cutoff'])
    excel_filename = f"pending_si_report_{datetime.now().strftime('%Y-%m-%d')}.xlsx"
    df.to_excel(excel_filename, index=False)
    return excel_filename

def send_pending_si_report():
    """
    Send a daily report at 6:00 PM IST with bookings where SI cutoff is within the next 24 hours.
    Includes the records in the email body as an HTML table and attaches an Excel file.
    """
    try:
        pending_si_data = fetch_pending_si_data()
        if not pending_si_data:
            print("No bookings with SI cutoff within the next 24 hours.")
            return

        # Sort the data by ETD (earliest first)
        pending_si_data.sort(key=lambda x: pd.to_datetime(x.get('ETD', ''), errors='coerce'))

        for booking in pending_si_data:
            print(f"Pending SI booking: {booking}")

        excel_file = generate_pending_si_excel(pending_si_data)
        if not excel_file:
            print("Failed to generate Excel file for pending SI report.")
            return

        sender_email, sender_password = SENDER_EMAIL_MUMBAI, SENDER_PASSWORD_MUMBAI
        msg = MIMEMultipart('alternative')
        msg['From'] = sender_email
        msg['To'] = ", ".join(["info@dessertmarine.com", "doc@dessertmarine.com"])
        msg['Cc'] = "chirag@dessertmarine.com"
        msg['Subject'] = f"PENDING SI : | {datetime.now().strftime('%Y-%m-%d')}"

        plain_body = """
Dear Team,

Please find below the list of bookings with SI cutoff dates within the next 24 hours.

"""
        for booking in pending_si_data:
            plain_body += f"""
Booking No: {booking['Booking No']}
Customer: {booking['Customer']}
FPOD: {booking['FPOD']}
Equipment Type: {booking['Equipment Type']}
Vessel: {booking['Vessel']}
ETD: {booking['ETD']}
SI Cutoff: {booking['SI Cutoff']}
"""
        plain_body += """
An Excel file with the details is also attached.

Note: This is an Auto Generated Mail.
"""
        msg.attach(MIMEText(plain_body, 'plain'))

        html_body = f"""
<html>
<body style="font-family: Arial, sans-serif;">
    <p>Dear Team,</p>
    <p>Please find below the list of bookings with SI cutoff dates within the next 24 hours.</p>
    <table border="1" cellpadding="5" cellspacing="0" style="border-collapse: collapse;">
        <tr style="background-color: #f2f2f2;">
            <th>Booking No</th>
            <th>Customer</th>
            <th>FPOD</th>
            <th>Equipment Type</th>
            <th>Vessel</th>
            <th>ETD</th>
            <th>SI Cutoff</th>
        </tr>
"""
        for booking in pending_si_data:
            html_body += f"""
        <tr>
            <td>{booking['Booking No']}</td>
            <td>{booking['Customer']}</td>
            <td>{booking['FPOD']}</td>
            <td>{booking['Equipment Type'] if booking['Equipment Type'] else 'N/A'}</td>
            <td>{booking['Vessel']}</td>
            <td>{booking['ETD']}</td>
            <td>{booking['SI Cutoff']}</td>
        </tr>
"""
        html_body += """
    </table>
    <p>An Excel file with the details is also attached.</p>
    <p><em>Note: This is an Auto Generated Mail.</em></p>
</body>
</html>
"""
        msg.attach(MIMEText(html_body, 'html'))

        with open(excel_file, 'rb') as f:
            attachment = MIMEApplication(f.read(), _subtype="xlsx")
            attachment.add_header('Content-Disposition', 'attachment', filename=excel_file)
            msg.attach(attachment)

        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(sender_email, normalized_app_password(sender_password))
            recipients = ["info@dessertmarine.com", "doc@dessertmarine.com", "chirag@dessertmarine.com"]
            server.sendmail(sender_email, recipients, msg.as_string())
            print(f"Pending SI report sent to {msg['To']} (CC: {msg['Cc']})")

        os.remove(excel_file)

    except Exception as e:
        print(f"Error sending pending SI report: {str(e)}")

def fetch_royal_castor_data():
    """
    Fetch bookings for Royal Castor where referenceNo exists.
    Returns a list of dictionaries with the required fields.
    """
    try:
        bookings_ref = db.collection("entries")
        docs = bookings_ref.stream()
        royal_castor_data = []

        for doc in docs:
            entry = doc.to_dict()
            entry["id"] = doc.id

            customer = entry.get("customer", {})
            if not isinstance(customer, dict):
                print(f"Skipping entry {entry['id']}: 'customer' field is not a dictionary, found {type(customer)}: {customer}")
                continue

            customer_name = customer.get("name", "")
            if "ROYAL CASTOR" in customer_name.upper():
                print(f"Found Royal Castor booking: {entry['id']}, bookingNo: {entry.get('bookingNo', 'N/A')}")
            else:
                continue

            reference_no = entry.get("referenceNo", "")
            if not reference_no:
                print(f"No referenceNo found for entry {entry['id']}")
                continue

            container_no = ""
            if "equipmentDetails" in entry and entry["equipmentDetails"]:
                if isinstance(entry["equipmentDetails"], list):
                    container_no = ", ".join(
                        eq.get("containerNo", "") for eq in entry["equipmentDetails"] if eq.get("containerNo")
                    )
                else:
                    print(f"Entry {entry['id']}: 'equipmentDetails' is not a list, found {type(entry['equipmentDetails'])}")
                    container_no = entry.get("containerNo", "")

            etd = entry.get("etd", "")
            if etd:
                try:
                    etd = pd.to_datetime(etd).strftime('%d-%m-%Y')
                except Exception as e:
                    print(f"Error parsing ETD for entry {entry['id']}: {e}")
                    etd = ""

            booking_data = {
                "Customer": customer_name,
                "Line": entry.get("line", ""),
                "Reference No": reference_no,
                "Booking No": entry.get("bookingNo", ""),
                "Container No": container_no,
                "Vessel": entry.get("vessel", ""),
                "ETD": etd,
                "Customer Email": customer.get("customerEmail", ["UJWALA@ROYALCASTOR.IN"])[0]
            }
            royal_castor_data.append(booking_data)

        return royal_castor_data

    except Exception as e:
        print(f"Error fetching Royal Castor data: {str(e)}")
        return []

def send_royal_castor_vessel_update():
    """
    Send a daily vessel update email at 7:30 PM IST to Royal Castor for bookings with a referenceNo.
    """
    try:
        royal_castor_data = fetch_royal_castor_data()
        if not royal_castor_data:
            print("No bookings for Royal Castor with referenceNo.")
            return

        # Sort the data by ETD (earliest to latest)
        royal_castor_data.sort(key=lambda x: pd.to_datetime(x.get('ETD', ''), errors='coerce'))

        for booking in royal_castor_data:
            print(f"Royal Castor booking: {booking}")

        sender_email, sender_password = SENDER_EMAIL_MUMBAI, SENDER_PASSWORD_MUMBAI
        customer_email = royal_castor_data[0]["Customer Email"]
        msg = MIMEMultipart('alternative')
        msg['From'] = sender_email
        msg['To'] = customer_email
        msg['Cc'] = "info@dessertmarine.com"
        msg['Subject'] = f"Daily Vessel Update : {datetime.now().strftime('%Y-%m-%d')} || Royal Castor"

        plain_body = """
Dear Royal Castor Team,

Please find below the daily vessel update.

"""
        for booking in royal_castor_data:
            plain_body += f"""
Customer: {booking['Customer']}
Line: {booking['Line']}
Reference No: {booking['Reference No']}
Booking No: {booking['Booking No']}
Container No: {booking['Container No']}
Vessel: {booking['Vessel']}
ETD: {booking['ETD']}
"""
        plain_body += """
Note: This is an Auto Generated Mail.
"""
        msg.attach(MIMEText(plain_body, 'plain'))

        html_body = f"""
<html>
<body style="font-family: Arial, sans-serif;">
    <p>Dear Royal Castor Team,</p>
    <p>Please find below the daily vessel update.</p>
    <table border="1" cellpadding="5" cellspacing="0" style="border-collapse: collapse;">
        <tr style="background-color: #f2f2f2;">
            <th>Customer</th>
            <th>Line</th>
            <th>Reference No</th>
            <th>Booking No</th>
            <th>Container No</th>
            <th>Vessel</th>
            <th>ETD</th>
        </tr>
"""
        for booking in royal_castor_data:
            html_body += f"""
        <tr>
            <td>{booking['Customer']}</td>
            <td>{booking['Line'] if booking['Line'] else 'N/A'}</td>
            <td>{booking['Reference No']}</td>
            <td>{booking['Booking No']}</td>
            <td>{booking['Container No'] if booking['Container No'] else 'N/A'}</td>
            <td>{booking['Vessel']}</td>
            <td>{booking['ETD']}</td>
        </tr>
"""
        html_body += """
    </table>
    <p><em>Note: This is an Auto Generated Mail.</em></p>
</body>
</html>
"""
        msg.attach(MIMEText(html_body, 'html'))

        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(sender_email, normalized_app_password(sender_password))
            recipients = [customer_email, "chirag@dessertmarine.com"]
            server.sendmail(sender_email, recipients, msg.as_string())
            print(f"Royal Castor vessel update sent to {msg['To']} (CC: {msg['Cc']})")

    except Exception as e:
        print(f"Error sending Royal Castor vessel update: {str(e)}")

@app.route('/api/send-sob-email', methods=['POST'])
def send_sob_email():
    """
    Sends SOB email using branch sender chosen by LOCATION stored in Firestore.
    Priority to determine location:
      1) Firestore entry by id (preferred)
      2) Firestore entry by bookingNo (fallback)
      3) Request body 'location' (last resort, defaults to MUMBAI)
    Accepts customer_email / sales_person_email as list or string.
    """
    try:
        data = request.get_json()
        print(f"Received data: {data}")

        # ---------- Inputs from request (used for email content) ----------
        booking_id = data.get('id') or data.get('entry_id')
        booking_no = data.get('booking_no') or data.get('bookingNo') or ''
        customer_email = data.get('customer_email')
        sales_person_email = data.get('sales_person_email')
        customer_name = data.get('customer_name')
        sob_date = data.get('sob_date')
        vessel = data.get('vessel')
        voyage = data.get('voyage')
        pol = data.get('pol')
        pod = data.get('pod')
        fpod = data.get('fpod', '')
        container_no = data.get('container_no')
        volume = data.get('volume')
        bl_no = data.get('bl_no', '')

        # ---------- Find LOCATION from Firestore ----------
        def _extract_loc_str(loc_val):
            if isinstance(loc_val, dict):
                return (loc_val.get('name') or '').strip()
            return (loc_val or '').strip()

        location_from_db = None

        # Prefer lookup by Firestore document id
        if booking_id:
            try:
                doc_ref = db.collection("entries").document(booking_id)
                doc = doc_ref.get()
                if doc.exists:
                    entry = doc.to_dict()
                    location_from_db = _extract_loc_str(entry.get('location'))
                    print(f"[SOB] Location by id {booking_id}: {location_from_db}")
                else:
                    print(f"[SOB] No Firestore entry for id {booking_id}")
            except Exception as e:
                print(f"[SOB] Error fetching entry by id {booking_id}: {e}")

        # Fallback: lookup by bookingNo if still unknown
        if not location_from_db and booking_no:
            try:
                query_ref = db.collection("entries").where("bookingNo", "==", booking_no).limit(1)
                docs = list(query_ref.stream())
                if docs:
                    entry = docs[0].to_dict()
                    location_from_db = _extract_loc_str(entry.get('location'))
                    print(f"[SOB] Location by bookingNo {booking_no}: {location_from_db}")
                else:
                    print(f"[SOB] No Firestore entry for bookingNo {booking_no}")
            except Exception as e:
                print(f"[SOB] Error querying by bookingNo {booking_no}: {e}")

        # Last resort: trust request body (or default MUMBAI)
        location = location_from_db or _extract_loc_str(data.get('location')) or "MUMBAI"
        print(f"[SOB] Using location: {location}")

        # ---------- Coerce email arrays ----------
        def _coerce_emails(val):
            if not val:
                return []
            if isinstance(val, list):
                return [e.strip() for e in val if str(e).strip()]
            if isinstance(val, str):
                parts = [p.strip() for p in val.split(",")]
                return [p for p in parts if p]
            return [str(val).strip()] if str(val).strip() else []

        customer_emails = _coerce_emails(customer_email)
        sales_person_emails = _coerce_emails(sales_person_email)

        if not customer_emails or not sales_person_emails:
            print("[SOB] Missing customer_email or sales_person_email")
            return jsonify({"error": "Customer or salesperson email missing"}), 400

        # ---------- Container no formatting ----------
        if container_no is None:
            container_no_str = ""
        elif isinstance(container_no, list):
            container_no_str = ", ".join(str(c) for c in container_no if c)
        elif isinstance(container_no, str):
            container_no_str = container_no
        else:
            container_no_str = str(container_no)

        # ---------- Pick sender based on LOCATION from DB ----------
        sender_email, sender_password = get_sender_by_location(location)

        # ---------- Compose mail ----------
        msg = MIMEMultipart('alternative')
        msg['From'] = sender_email
        msg['To'] = ", ".join(customer_emails)
        msg['Cc'] = ", ".join(sales_person_emails)
        msg['Subject'] = f"{customer_name} | SHIPPED ON BOARD | {vessel} | {booking_no} | {bl_no}"

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

        print(f"[SOB] Sending from {sender_email} (location: {location}) to {customer_emails} CC {sales_person_emails}")
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            print("[SOB] TLS started")
            server.login(sender_email, normalized_app_password(sender_password))
            print("[SOB] Login successful")
            recipients = customer_emails + sales_person_emails
            server.sendmail(sender_email, recipients, msg.as_string())
            print("[SOB] Email sent successfully")

        return jsonify({"message": "Email sent successfully"}), 200

    except Exception as e:
        print(f"[SOB] Error sending email: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/send-selling-email', methods=['POST'])
def send_selling_email():
    try:
        data = request.get_json()
        print(f"Received data for selling email: {data}")

        bl_no = data.get('bl_no', '')
        booking_no = data.get('booking_no', '')
        customer_name = data.get('customer_name', '')
        pol = data.get('pol', '')
        fpod = data.get('fpod', '')
        volume = data.get('volume', '')
        buy_rate = data.get('buy_rate', '')
        sell_rate = data.get('sell_rate', '')
        sales_person_email = data.get('sales_person_email', [])
        location = data.get('location', 'MUMBAI')

        if isinstance(sales_person_email, list):
            sales_person_email = sales_person_email[0] if sales_person_email else None

        if not sales_person_email:
            print("Missing sales_person_email")
            return jsonify({"error": "Salesperson email missing"}), 400

        msg = MIMEMultipart('alternative')
        sender_email, sender_password = get_sender_by_location(location)
        msg['From'] = sender_email
        msg['To'] = ", ".join(["manas.jadhav.7779@gmail.com", "tech.manasjadhav@gmail.com"])
        msg['Cc'] = sales_person_email
        msg['Subject'] = f"Selling | {bl_no}"

        plain_body = f"""
Dear Team,

Please find below the details for the selling rate:

BL/NO: {bl_no}
BOOKING NO: {booking_no}
CUSTOMER: {customer_name}
POL: {pol}
FPOD: {fpod}
VOLUME: {volume}
BUY RATE: {buy_rate}
SELL RATE: {sell_rate}

Note: This is an Auto Generated Mail.
"""
        msg.attach(MIMEText(plain_body, 'plain'))

        html_body = f"""
<html>
<body style="font-family: Arial, sans-serif;">
    <p>Dear Team,</p>
    <p>Please find below the details for the selling rate:</p>
    <table border="1" cellpadding="5" cellspacing="0" style="border-collapse: collapse;">
        <tr style="background-color: #f2f2f2;">
            <th>BL/NO</th>
            <th>BOOKING NO</th>
            <th>CUSTOMER</th>
            <th>POL</th>
            <th>FPOD</th>
            <th>VOLUME</th>
            <th>BUY RATE</th>
            <th>SELL RATE</th>
        </tr>
        <tr>
            <td>{bl_no}</td>
            <td>{booking_no}</td>
            <td>{customer_name}</td>
            <td>{pol}</td>
            <td>{fpod}</td>
            <td>{volume}</td>
            <td>{buy_rate}</td>
            <td>{sell_rate}</td>
        </tr>
    </table>
    <p><em>Note: This is an Auto Generated Mail.</em></p>
</body>
</html>
"""
        msg.attach(MIMEText(html_body, 'html'))

        print(f"Attempting to send selling email from {sender_email} to {msg['To']} with CC {sales_person_email}")
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(sender_email, normalized_app_password(sender_password))
            recipients = ["manas.jadhav.7779@gmail.com", "tech.manasjadhav@gmail.com", sales_person_email]
            server.sendmail(sender_email, recipients, msg.as_string())
            print("Selling email sent successfully")

        return jsonify({"message": "Selling email sent successfully"}), 200

    except Exception as e:
        print(f"Error sending selling email: {str(e)}")
        return jsonify({"error": str(e)}), 500

def fetch_bookings_by_salesperson():
    try:
        bookings_ref = db.collection("entries")
        docs = bookings_ref.stream()
        bookings_by_salesperson = {}

        for doc in docs:
            entry = doc.to_dict()
            entry["id"] = doc.id
            print(f"Entry {entry['id']}: {entry}")

            customer = entry.get("customer", {})
            if not isinstance(customer, dict):
                print(f"Skipping entry {entry['id']}: 'customer' field is not a dictionary, found {type(customer)}: {customer}")
                continue

            sales_person_emails = customer.get("salesPersonEmail", [])
            if not sales_person_emails:
                print(f"No salesperson email found for entry {entry['id']}")
                continue

            container_no = ""
            if "equipmentDetails" in entry and entry["equipmentDetails"]:
                if isinstance(entry["equipmentDetails"], list):
                    container_no = ", ".join(
                        eq["containerNo"] for eq in entry["equipmentDetails"] if eq.get("containerNo")
                    )
                else:
                    print(f"Skipping entry {entry['id']}: 'equipmentDetails' is not a list, found {type(entry['equipmentDetails'])}")
            else:
                container_no = entry.get("containerNo", "")

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
                "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "Pending SI": "Yes" if not entry.get("siFiled", False) else "No",
                "Pending BL": "Yes" if not entry.get("blReleased", False) else "No",
                "Location": entry.get("location", "")
            }

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
    if not bookings:
        print(f"No bookings found for salesperson {salesperson_email}")
        return None

    df = pd.DataFrame(bookings)
    # Add SI Cutoff column if not present
    if 'SI Cutoff' not in df.columns:
        si_cutoff_list = []
        for b in bookings:
            val = b.get('SI Cutoff') or b.get('siCutOff') or ''
            if isinstance(val, (str, type(None))):
                si_cutoff_list.append(val)
            elif hasattr(val, 'strftime'):
                si_cutoff_list.append(val.strftime('%d/%m/%Y %H:%M'))
            else:
                si_cutoff_list.append(str(val))
        df['SI Cutoff'] = si_cutoff_list
    # Sort by ETD if present, else by SI Cutoff
    sort_cols = []
    if 'ETD' in df.columns:
        sort_cols.append('ETD')
    if 'SI Cutoff' in df.columns:
        sort_cols.append('SI Cutoff')
    if sort_cols:
        for col in sort_cols:
            df[col] = pd.to_datetime(df[col], errors='coerce')
        df = df.sort_values(by=sort_cols, ascending=True)
        # Format back to string for Excel
        for col in sort_cols:
            df[col] = df[col].dt.strftime('%d-%m-%Y')
    # Format all date columns
    date_columns = [col for col in df.columns if 'date' in col.lower() or col.upper() == 'ETD' or col.upper() == 'SI CUTOFF']
    for col in date_columns:
        df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%d-%m-%Y')
    excel_filename = f"booking_report_{salesperson_email.split('@')[0]}_{datetime.now().strftime('%Y-%m-%d')}.xlsx"
    df.to_excel(excel_filename, index=False)
    return excel_filename

def send_daily_report():
    try:
        bookings_by_salesperson = fetch_bookings_by_salesperson()
        if not bookings_by_salesperson:
            print("No bookings found for any salesperson.")
            return
        for salesperson_email, loc_dict in bookings_by_salesperson.items():
            all_bookings = []
            for location, bookings in loc_dict.items():
                all_bookings.extend(bookings)
            if not all_bookings:
                continue
            print(f"Generating report for {salesperson_email} with {len(all_bookings)} bookings (all locations)")
            excel_file = generate_excel_report(salesperson_email, all_bookings)
            if not excel_file:
                continue
            sender_email, sender_password = get_sender_by_location(all_bookings[0].get('Location', ''))
            msg = MIMEMultipart()
            msg['From'] = sender_email
            msg['Subject'] = f"Daily Booking Report - {datetime.now().strftime('%Y-%m-%d')}"

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
                server.login(sender_email, normalized_app_password(sender_password))
                server.sendmail(sender_email, all_sales_emails, msg.as_string())
                print(f"Daily report sent to {salesperson_email} (all locations)")
            os.remove(excel_file)
    except Exception as e:
        print(f"Error sending daily reports: {str(e)}")

# Schedule tasks
#schedule.every().day.at("13:30").do(send_daily_report)
schedule.every().hour.do(send_si_cutoff_reminder)
#schedule.every().day.at("12:30").do(send_pending_si_report)
#schedule.every().day.at("13:30").do(send_royal_castor_vessel_update)

def run_scheduler():
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == '__main__':
    import os
    if os.environ.get('RUN_SCHEDULER', 'false').lower() == 'true':
        print(f"[WORKER] Starting scheduler with SENDER_EMAIL_MUMBAI: {SENDER_EMAIL_MUMBAI}")
        scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
        scheduler_thread.start()
        while True:
            time.sleep(3600)
    else:
        print(f"[WEB] Starting Flask app with SENDER_EMAIL_MUMBAI: {SENDER_EMAIL_MUMBAI}")
        app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 10000)))
