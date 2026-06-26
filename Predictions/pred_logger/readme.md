## Prediction Logger

Consumes from `ml.pred.alert.eta` and stores predictions as daily Parquet files.

### Build & Run
```bash
docker build --no-cache -t mic/pred_logger:1.0.0 .
docker compose up -d
```
### Verify
```bash
docker logs pred_logger -f
ls -la /home/micml/Documents/TestML/predictions
```
### Read predictions
```bash
import pandas as pd
df = pd.read_parquet("/home/micml/Documents/TestML/predictions/predictions_2026-03-06.parquet")
print(df.shape)
print(df.head())
print(df['mc_no'].value_counts())
print(df['next_type'].value_counts())
```