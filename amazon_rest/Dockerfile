# Use an official Python base image (slim version to reduce size)
FROM python:3.11-slim

# Set the working directory inside the container
WORKDIR /app

# Copy resources to container
COPY . .

# Install dependencies without keeping the pip cache 
RUN pip install --no-cache-dir -r requirements.txt

# Set the Hadoop's password 
ENV HADOOP_PASSWORD="miranda"

# Expose the port on which the application will run 
EXPOSE 8080

# Command to start the application.
CMD ["flask", "--app", "app.py", "run", "--host=0.0.0.0", "--port=8080"]