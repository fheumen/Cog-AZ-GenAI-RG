# scheduler.py
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from utils import scan_and_update_ispr_status, scan_and_ingestion_and_generation
from utils import scan_and_update_ispr_status, scan_and_ingestion_and_generation
from clean_utils import delete_weekend_tmp_directories, delete_daily_tmp_directories, delete_all_under_prefix
from template.template_utils import scan_and_update_template_status
from loguru import logger
import pytz

##new scheduler initiated
scheduler = BackgroundScheduler(timezone=pytz.timezone("Europe/London"))

def start_job(default_n_mins, default_n_queue_mins, report_table_name, reportqueue_table_name, table_template_master, knowledge_base_name, model_id, bucket_name, output_folder, input_folder,  list_site_names, list_product_names, date_pattern, header_pattern, footer_pattern, email_sender, kbname_max_size, knowledge_base_name_report):
    """Start the scheduled job that runs every `default_n_mins` minutes"""
    # print("scheduler.start_job --------------->")
    
    # Remove existing jobs before adding a new one (optional, to avoid duplicates)
    scheduler.remove_all_jobs()

    #Schedule the job
    logger.info("scan_and_update_ispr_status started")
    scheduler.add_job(
        scan_and_update_ispr_status, 
        trigger=IntervalTrigger(minutes=default_n_mins), 
        args=[default_n_mins, report_table_name],
        id="my_scheduled_task_for_ispr",
        replace_existing=True
    )

    # logger.info("scan_and_ingestion_and_generation")

      #Schedule the job
    logger.info("scan_and_update_itemplate_status started")
    scheduler.add_job(
        scan_and_update_template_status, 
        trigger=IntervalTrigger(minutes=default_n_mins), 
        args=[default_n_mins, table_template_master],
        id="my_scheduled_task_for_template",
        replace_existing=True
    )

    logger.info("scan_and_ingestion_and_generation")
    
    # Schedule the job
    scheduler.add_job(
        scan_and_ingestion_and_generation, 
        trigger=IntervalTrigger(minutes=default_n_queue_mins), 
        args=[default_n_queue_mins, reportqueue_table_name, table_template_master, knowledge_base_name, model_id, bucket_name, input_folder, output_folder, list_site_names, list_product_names, date_pattern, header_pattern, footer_pattern, email_sender, kbname_max_size, knowledge_base_name_report],
        id="my_scheduled_task_for_svr",
        replace_existing=True
    )

    #Schedule the job
    logger.info("cleaning temporary files")
    scheduler.add_job(
        delete_daily_tmp_directories, 
        trigger=CronTrigger(hour=3, minute=0),   # runs at 03:00 AM UK Time every day 
        args=[bucket_name],
        id="my_scheduled_task_for_cleaning",
        replace_existing=True
    )

    
 
    if not scheduler.running:
        scheduler.start()