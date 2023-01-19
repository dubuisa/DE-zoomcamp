**Question 1** - Which tag on the "docker build" help has the following text? - *Write the image ID to the file* ?  

```bash
docker build --help | grep "Write the image ID to the file"
```

Answer is `--idimage string`

---


**Question 2** - How many python modules are installed on image python:3.9?

```bash
docker run -it python:3.9 pip list
```
Answer is `3`

---

**Question 3** - How many taxi trips were totally made on January 15?
```sql
select
	count(*)
from
	 trips t
where
	t.lpep_pickup_datetime::Date = '2019-01-15'::Date
	and t.lpep_dropoff_datetime::Date = '2019-01-15'::Date;
```
Answer is `20530`

---

**Question 4** - Which was the day with the largest trip distance?
```sql
select
	lpep_pickup_datetime::Date as pickup_date,
	max(trip_distance) as max_distance
from
	trips
group by
	pickup_date
order by
	max_distance desc
limit 1 ;
```
Answer is `2019-01-15`

---

**Question 5** - In 2019-01-01 how many trips had 2 and 3 passengers?
```sql
select
	passenger_count,
	count(*) as n_trips
from
	trips
where
	lpep_pickup_datetime::Date = '2019-01-01'::Date
	and passenger_count in (2, 3)
group by
	passenger_count
```
Answer is `2: 1282 ; 3: 254`

---

**Question 6** - For the passengers picked up in the Astoria Zone which was the drop up zone that had the largest tip?
```sql
select
	drop_loc."Zone" as drop_location
from
	trips t
join zones drop_loc on
	t."DOLocationID" = drop_loc."LocationID"
join zones pickup_loc on
	t."PULocationID" = pickup_loc."LocationID"
where pickup_loc."Zone" = 'Astoria'
group by drop_location
order by max(tip_amount) desc
limit 1;



```

Answer is `Long Island City/Queens Plaza`

---