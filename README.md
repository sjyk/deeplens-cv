# DeepLens

DeepLens is a temporally and spatially partitioned storage manager for video analytics.

# Installation

1. Install Dependencies

```bash
sudo apt update
sudo apt install postgresql postgresql-contrib
pip3 install -r requirements
```

2. Configure PostgreSQL

Make sure PostgreSQL server is running and we have an empty database `header` created.

3. Run experiments

`python3 experiments/exp1_traffic_camera.py https://example.com/video.mp4`
