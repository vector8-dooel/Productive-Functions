import logging
import azure.functions as func
from productive_client.pipeline import run_incremental_pipeline
from productive_client.config import CRON_SCHEDULE

app = func.FunctionApp()

@app.timer_trigger(schedule=CRON_SCHEDULE, arg_name="mytimer", use_monitor=True)
def productive_incremental_timer(mytimer: func.TimerRequest) -> None:
    logging.info("Starting Productive incremental refresh")
    run_incremental_pipeline()
    logging.info("Finished Productive incremental refresh")