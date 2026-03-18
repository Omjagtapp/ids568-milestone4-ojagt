# Milestone 4: Distributed & Streaming Pipeline
[![Python CI Pipeline](https://github.com/Omjagtapp/ids568-milestone4-ojagt/actions/workflows/ci.yml/badge.svg)](https://github.com/Omjagtapp/ids568-milestone4-ojagt/actions)

## Setup Instructions
1. Create a virtual environment: `python -m venv venv`
2. Activate the environment: 
   - Windows: `venv\Scripts\activate`
   - Mac/Linux: `source venv/bin/activate`
3. Install dependencies: `pip install -r requirements.txt`

## 1. Data Generation
Generate 10 million rows of reproducible synthetic data:
```bash
python generate_data.py --rows 10000000 --output data/raw --seed 42