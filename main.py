from fastapi import FastAPI, HTTPException
import uuid
import threading
from report_generation import generate_report

app = FastAPI()

# In-memory storage for report status and result
REPORTS = {}

@app.post("/trigger_report")
async def trigger_report():
    report_id = str(uuid.uuid4())

    # Create a new report and add it to the reports dict with status "Running"
    REPORTS[report_id] = {"status": "Running"}
    print(f"New report triggered with ID: {report_id}")

    def run_report():
        # Generate the report
        result = generate_report()

        # Update the status of the report to "Complete" and add the result
        REPORTS[report_id] = {"status": "Complete", "result": result}
        print(f"Report {report_id} completed with status {REPORTS[report_id]['status']}")

    # Start a new thread to generate the report
    threading.Thread(target=run_report).start()

    # Return the report ID
    return {"report_id": report_id}

@app.get("/get_report/{report_id}")
async def get_report(report_id: str):
    # Check if the report ID is valid
    if report_id not in REPORTS:
        raise HTTPException(status_code=404, detail="Report not found")

    report = REPORTS[report_id]

    # If the report is still running, return status "Running"
    if report["status"] == "Running":
        print(f"Report {report_id} is still running")
        return {"status": "Running"}
    else:
        # If the report is complete, return status "Complete" and the report result
        print(f"Report {report_id} is complete")
        return {"status": "Complete", "result": report["result"]}
