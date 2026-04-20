# 1. Use a lightweight Python base image
FROM python:3.11-slim

# 2. Set the working directory
WORKDIR /app

# 3. Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 4. Copy the requirements file
COPY requirements.txt .

# 5. Install Python libraries
RUN pip install --no-cache-dir -r requirements.txt

# 6. Copy application code and the model file

COPY streamlit.py .
COPY hybrid_recommender_full.pkl .

# 7. Expose the port
EXPOSE 8501

# 8. Command to run the app (Changed app.py to streamlit.py)
ENTRYPOINT ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
