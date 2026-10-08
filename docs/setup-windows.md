# Running ReviewLens on Windows (8 GB RAM laptop)

Everything here is free. Total disk needed is about 8 GB (Docker + images + data).

## 1. Install WSL2 + Docker Desktop (once, ~30 min, needs admin + a reboot)
1. Open **PowerShell as Administrator** and run `wsl --install`. Reboot when it asks.
2. Install **Docker Desktop** from docker.com. It's free for personal use. During setup, keep **"Use WSL 2 based engine"** ticked.
3. Start Docker Desktop and wait for "Engine running".

## 2. Give Docker enough memory, but not all of it
By default WSL2 gets 50% of RAM (~3.9 GB), which is too little for this stack. Create the file `C:\Users\<you>\.wslconfig` with:

```ini
[wsl2]
memory=5GB
swap=4GB
processors=4
```

Then in PowerShell run `wsl --shutdown` and restart Docker Desktop. That leaves about 3 GB for Windows. **Close Chrome and other heavy apps while the stack runs.**

## 3. Put the project on a short path
Windows has a 260-character path limit that breaks some tools. Use something like `C:\projects\reviewlens`.

## 4. First run (~10–15 min: downloads images and Spark jars once)
```bash
cd C:\projects\reviewlens
docker compose up -d --build
docker compose ps
```
All services should show `running`/`healthy`, and `kafka-init` should show `exited (0)`.

- Airflow: http://localhost:8080. DAG `reviewlens_pipeline` starts unpaused and runs every 15 minutes. Press ▶ to run it now.
- Dashboard: http://localhost:8501. Data appears after the first successful DAG run.

## 5. Day to day
```bash
docker compose stop                 # free the RAM, keep all data
docker compose start                # resume
docker compose stop producer        # pause the event stream only
docker compose logs -f airflow      # Spark output from tasks also appears in the Airflow UI logs
docker compose down -v              # wipe everything (Kafka, lake, Airflow DB) and start fresh
```

## Troubleshooting
| Symptom | Fix |
|---|---|
| Laptop freezes, containers restart | Not enough RAM. Close apps. Lower `EVENTS_PER_SEC`. Check `.wslconfig` was applied (`wsl --shutdown`, then restart Docker) |
| `airflow` exits with code 137 | Out of memory (OOM kill). Same as above. You can also set `SPARK_DRIVER_MEMORY=768m` in docker-compose.yml |
| Task `kafka_to_bronze` fails with a connection error | Kafka still starting. The task retries automatically, or check `docker compose ps kafka` |
| Dashboard says "No published gold data yet" | Trigger the DAG and wait ~3–5 min for all 4 tasks to finish |
| Gold task fails "quality gate failed" | Working as designed. Open the task log to see which check failed, then follow [the runbook](ownership/slos-and-runbook.md) |
