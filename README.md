# Amazon Reviews Project
*Cloud Computing Project — Academic Year 2025/2026*

Welcome to the repository of the **MirandaPriestley Group**, created for our Cloud Computing final project.

**Team Members:**
* Erica Ginevri
* Chiara Nasoni
* Vittoria Vellutini


## 📁 Project Structure

The files and directories for this project are hosted on the Virtual Machine (VM) with the IP address **10.1.1.124**, which acts as a *datanode-namenode* within our Hadoop environment. 

The layout is divided between the `root` and `hadoop` user environments:

* The `root` user directory (accessible via `ssh root@10.1.1.124` and password `ubuntu`) contains:
  * `amazon-rest/`: which contains all the files and components for both the REST server and the REST client.

* The `hadoop` user directory (accessible via `sudo su -- hadoop` and password `miranda`) contains:
  * `amazon_project/`: which is the main directory containing the components as follow:
    * `spark/`: contains amazon_spark.py
    * `amazon_project/`: contains all Hadoop-related files
    * `python_parallel_execution.py`: a Python script for managing Hadoop parallel execution


## 🚀 Getting Started

To run the project and start the REST server, follow these steps:

1. Activate the VPN
2. Access the Virtual Machine
3. Navigate to the REST directory:
```bash
cd amazon_rest
```
4. Build the Docker image: compile the container for the REST server
```bash
docker build -t rest-server .
```
5. Run the server: start the container and map port 8080
```bash
docker run -p 8080:8080 rest-server
```
6. Access the Application: once the server is running, you can interact with the REST API by navigating to the following URL in your web browser:
[http://10.1.1.124:8080]