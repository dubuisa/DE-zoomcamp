**Question 1** - How many rows does that dataset have?  

```bash
prefect deployment run etl-parent-flow/docker-flow -p "year=2020" -p "months=[1]" -p "color=green"
```

Answer is `447,770`

---


**Question 2** - create a deployment to run on the first of every month at 5am UTC. What’s the cron schedule for that?
```python
from etl_web_to_gcs import etl_parent_flow
from prefect.orion.schemas.schedules import CronSchedule
from prefect.deployments import Deployment

## https://crontab.guru/#0_5_1_*_*

cron_demo = Deployment.build_from_flow(
    etl_parent_flow,
    "web to gcs with cron scheduler",
    schedule=(CronSchedule(cron="0 5 1 * *"))
)

if __name__ == '__main__':
    cron_demo.apply()

```
Answer is `0 5 1 * *`

---

**Question 3** - Loading Data to Big Query

```bash
prefect deployment build flows/02_gcp/etl_web_to_gcs.py:etl_web_to_gcs_parent_flow --apply --name flow
prefect deployment run etl-web-to-gcs-parent-flow/flow -p "year=2019" -p "months=[2,3]" -p "color=yellow"

prefect deployment build flows/02_gcp/etl_gcs_to_bq.py:gcs_to_bq_parent_flow --apply --name flow
prefect deployment run gcs-to-bq-parent-flow/flow -p "year=2019" -p "months=[2,3]" -p "color=yellow"
```
Answer is `14,851,920`

---

**Question 4** - 

Create github block
```python3

from prefect.filesystems import GitHub

gh = GitHub(
    repository="https://github.com/dubuisa/DE-zoomcamp", reference="master"
)
gh.save("zoomcamp", overwrite=True)

```

Deploy flow using github block
```bash
python3 blocks/make_github_block.py
```

From this repo root directory
```
prefect deployment build ./week-2/prefect/flows/02_gcp/etl_web_to_gcs.py:etl_web_to_gcs \
  -n web-to-gcs-gh-source \
  -sb "github/zoomcamp" \
  --apply


prefect deployment run etl-web-to-gcs/web-to-gcs-gh-source \
  -p "year=2020" -p "month=11" -p "color=green"
```
Answer is `88,605`

---

**Question 5** - Email or Slack notifications
```bash
prefect deployment run etl-web-to-gcs/web-to-gcs-gh-source -p "year=2019" -p "month=4" -p "color=green"
```
Answer is `514392`

---

**Question 6** -  Secrets: how many characters are shown as asterisks (*)?

create secret with the UI
```
Value


********
```

Answer is `8`

---

