# FEFU_data_analytics

Assignment for the data analytics course

## Project Overview

This project simulates a banking system with:
- User accounts and profiles
- Banking operations
- Scheduled payments
- User achievements system
- Real-time data generation
- Redash dashboard for data visualization

## Quick start

1. Clone the repository to your computer:
```bash
git clone https://github.com/deep-learning-engineer/FEFU_data_analytics.git
```

2. Run containers with the command:
```bash
docker compose up -d
```

To run Jupter notebook for analysis, you also need to complete the following steps:
```bash
cd analysis
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```
P.s If you are using Windows, then use to activate the environment: ```.venv\Scripts\activate```

When launching Jupyter notebook, select the created environment as the Kernel.


## Database architecture
![UML](./assets/UML-MobileBank.jpg)

## Dashboard Examples

**Transaction Dashboard:** 
![Dashboard](./assets/Dashboard_Transactions.png)

**User Dashboard:**
![Dashboard](./assets/Dashboard_Users.png)
