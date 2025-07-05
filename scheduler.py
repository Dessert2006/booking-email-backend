from app import send_daily_report, send_pending_si_report, send_royal_castor_vessel_update
import schedule
import time

# Schedule tasks
schedule.every().day.at("19:00").do(send_daily_report)
schedule.every().day.at("19:00").do(send_pending_si_report)
schedule.every().day.at("19:00").do(send_royal_castor_vessel_update)

def run_scheduler():
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    print("[SCHEDULER] Starting scheduler process...")
    run_scheduler()