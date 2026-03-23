import schedule
import time
import os

def job():
    print("🔁 Mise à jour Tayara...")
    os.system("python scraper_list.py")
    os.system("python scrapper.py")

schedule.every().day.at("03:00").do(job)   # 3h du matin

print("⏰ Scheduler actif...")

while True:
    schedule.run_pending()
    time.sleep(60)
