#use an official Python image
FROM python:3.11-slim

#set the working directory inside the container
WORKDIR /app

#copy resources to container
COPY . .

#install dependencies
RUN pip install --no-cache-dir -r requirements.txt

#set the Hadoop's password 
ENV HADOOP_PASSWORD="miranda"

#expose the port on which the application will run 
EXPOSE 8080

#command to start the application
CMD ["flask", "--app", "app.py", "run", "--host=0.0.0.0", "--port=8080"]