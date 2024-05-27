FROM python:3.9

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire etl_project folder into the container
COPY etl_project ./etl_project

# Set the PYTHONPATH environment variable inside container to ensure correct imports
# when the container is running, Python will use /app as part of its module search path
ENV PYTHONPATH=/app

CMD ["python", "-m", "etl_project.pipelines.run"]
