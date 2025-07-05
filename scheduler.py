from app import send_daily_report, send_pending_si_report, send_royal_castor_vessel_update
import schedule
import time

# Wrapper functions with print statements

def run_send_daily_report():
    print("[SCHEDULER] Running send_daily_report...")
    try:
        send_daily_report()
        print("[SCHEDULER] send_daily_report completed.")
    except Exception as e:
        print(f"[SCHEDULER] Error in send_daily_report: {e}")

def run_send_pending_si_report():
    print("[SCHEDULER] Running send_pending_si_report...")
    try:
        send_pending_si_report()
        print("[SCHEDULER] send_pending_si_report completed.")
    except Exception as e:
        print(f"[SCHEDULER] Error in send_pending_si_report: {e}")

def run_send_royal_castor_vessel_update():
    print("[SCHEDULER] Running send_royal_castor_vessel_update...")
    try:
        send_royal_castor_vessel_update()
        print("[SCHEDULER] send_royal_castor_vessel_update completed.")
    except Exception as e:
        print(f"[SCHEDULER] Error in send_royal_castor_vessel_update: {e}")

# Schedule tasks
schedule.every().day.at("13:40").do(run_send_daily_report)
schedule.every().day.at("13:40").do(run_send_pending_si_report)
schedule.every().day.at("13:40").do(run_send_royal_castor_vessel_update)

def run_scheduler():
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    print("[SCHEDULER] Starting scheduler process...")
    run_scheduler()