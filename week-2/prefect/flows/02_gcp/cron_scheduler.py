from etl_web_to_gcs import etl_parent_flow
from prefect.orion.schemas.schedules import CronSchedule
from prefect.deployments import Deployment

cron_demo = Deployment.build_from_flow(
    etl_parent_flow,
    "web to gcs with cron scheduler",
    schedule=(CronSchedule(cron="0 5 1 * *"))
)

if __name__ == '__main__':
    cron_demo.apply()