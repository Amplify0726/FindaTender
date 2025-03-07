from flask import Flask, jsonify
import schedule
import time
import threading

app = Flask(__name__)

# Basic route to ensure no 404 error at the root
@app.route('/')
def home():
    return "Welcome to the Find a Tender job service!"

# The job that runs your existing task
def run_job():
    # Your existing code here to extract data from Find a Tender and write to Google Sheets
    print("Job is running...")  # Replace this line with your existing job code

# Define a route to manually trigger the job
@app.route('/run_job', methods=['POST'])
def manual_trigger():
    run_job()
    return jsonify({"message": "Job triggered manually!"}), 200

# Set up the scheduler for automatic execution
def run_scheduled_jobs():
    # You can change the interval to any frequency you want (e.g., every 24 hours)
    schedule.every(24).hours.do(run_job)

    while True:
        schedule.run_pending()
        time.sleep(1)

# Start the scheduled job in a separate thread so it runs in the background
threading.Thread(target=run_scheduled_jobs, daemon=True).start()

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
