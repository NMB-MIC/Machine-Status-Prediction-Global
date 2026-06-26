Streamlit Dashboard — Upcoming Machine Alerts

Prereqs:
- Kafka predictions topic: ml.pred.alert.eta (JSON)
- Python 3.10+ on the same server
- Flink job and model service already running

Install:
cd dashboard
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt

Run:
export KAFKA_BOOTSTRAP=localhost:9092
export KAFKA_TOPIC=ml.pred.alert.eta
export BUFFER_HOURS=87600
streamlit run app.py --server.port 8501

Open:
http://localhost:8501

Tips:
- For shadow mode, set “Start from” to “earliest (replay)” in the sidebar to read older predictions.
- Use “Horizon (minutes)” to control how far into the future to look (P50 must be within this window).
- The “Time zone” toggle switches between your local time and UTC.
- Type is hidden if confidence < 0.6 (default). Adjust in the sidebar.