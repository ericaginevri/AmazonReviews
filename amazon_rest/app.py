import uuid
import time
from flask import Flask, request, jsonify, Response, send_file
import paramiko
import os
import requests

app = Flask(__name__) 

#global variables for authentication used by REST server to connect to the cluster
hostname = "10.1.1.124"
username = "hadoop"
password = os.getenv("HADOOP_PASSWORD")

#Hadoop

#jobs info
jar_name = "amazon_project-1.0-SNAPSHOT.jar"
job1_name = "Job1Aggregation"
job2_name = "Job2Ranking"
job3_name = "Job3Anomaly"

#this function executes a command on the Hadoop cluster using SSH
def execute_ssh_command(hostname, username, password, command):
    #initialize SSH client
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    #connection to remote server
    ssh.connect(hostname=hostname, username=username, password=password, timeout=15)
    
    #execution of command
    ssh.exec_command(command)
    ssh.close()

#function to find the job using its tag
def get_job_by_tag(tag):
    url = f"http://{hostname}:8088/ws/v1/cluster/apps"
    
    #parameters of query string (equivalent to ?applicationTags=<tag>)
    parameters = { "applicationTags": tag }

    try:
        response = requests.get(url, params=parameters, timeout=10) 
        response.raise_for_status() #if status not 200, throw an exception
        response_as_json = response.json()
        
        #list of jobs is returned as an object with schema { apps : { app : [<jobs>] }} 
        #to take a job we need to take apps, then app and then the first element (because tag is unique)
        apps_container = response_as_json.get("apps")
        
        if apps_container and apps_container.get("app"): 
            app_list = apps_container.get("app") #get the list of jobs
            app = app_list[0]
            return app
        else:
            return None

    except requests.exceptions.RequestException as e:
        print(f"Temporary error of connection to YARN: {e}")


#POST Endpoint to start a pipeline of jobs in Hadoop
@app.route('/jobs/hadoop', methods=['POST'])
def create_job():
    data = request.get_json() or {} 

    #input dataset
    dataset = data.get('dataset')

    if not dataset:
        return jsonify({
            "error": "Bad request",
            "message": "Dataset is required"
        }), 400

    #Job1 parameters
    job1_reducers = data.get('job1Reducers', 3)
    input_split_size = data.get('inputSplitSize', 134217728)
    #Job2 parameters
    job2_reducers = data.get('job2Reducers', 3)
    #Job3 parameters
    job3_reducers = data.get('job3Reducers', 3)
    min_reviews_perfect = data.get('minReviewsPerfect', 100)
    min_helpful_ratio = data.get('minHelpfulRatio', 0.2)
    min_verified_ratio = data.get('minVerifiedRatio', 0.5)

    pipeline_id = str(uuid.uuid4())

    job1_tag = f"{pipeline_id}_job1"
    job2_tag = f"{pipeline_id}_job2"
    job3_tag = f"{pipeline_id}_job3"

    #sequence of the three commands 
    inner = (
        #JOB 1 
        f"cd amazon_project/amazon_project/target && "
        f"/opt/hadoop/bin/hadoop jar {jar_name} "
        f"it.unipi.hadoop.Job1Aggregation "
        f"-D mapreduce.job.tags={job1_tag} "
        f"-D mapreduce.input.fileinputformat.split.maxsize={input_split_size} "
        f"{dataset} {job1_tag} {job1_reducers} && "
        #JOB 2
        f"/opt/hadoop/bin/hadoop jar {jar_name} "
        f"it.unipi.hadoop.Job2Ranking "
        f"-D mapreduce.job.tags={job2_tag} "
        f"{job1_tag} {job2_tag} {job2_reducers} && "
        #JOB 3
        f"/opt/hadoop/bin/hadoop jar {jar_name} "
        f"it.unipi.hadoop.Job3Anomaly "
        f"-D mapreduce.job.tags={job3_tag} "
        f"{job1_tag} {job2_tag} {job3_tag} "
        f"{job3_reducers} {min_reviews_perfect} {min_helpful_ratio} {min_verified_ratio}"
    )

    command = f"nohup sh -c '{inner}' > /home/hadoop/pipeline_{pipeline_id}.log 2>&1 < /dev/null &"

    execute_ssh_command(hostname, username, password, command)
 
    max_retries = 50
    seconds_to_wait = 5

    for _ in range(max_retries):
        app_yarn = get_job_by_tag(job1_tag)
        if app_yarn is not None:
            return jsonify({
                "message": "Pipeline started successfully",
                "pipelineId": pipeline_id,
                "job1Tag": job1_tag,
                "job2Tag": job2_tag,
                "job3Tag": job3_tag
            }), 201
        time.sleep(seconds_to_wait)

    return jsonify({
        "error": "Timeout",
        "message": "Command has been sent, but the job seems not to be started",
        "pipelineId": pipeline_id,
        "job1Tag": job1_tag,
        "job2Tag": job2_tag,
        "job3Tag": job3_tag
    }), 500 


#GET Endpoint to track progression of pipeline in Hadoop
@app.route('/jobs/hadoop/pipeline/<pipeline_id>', methods=['GET'])
def get_pipeline(pipeline_id):
    try:
        job1_data = get_job_by_tag(f"{pipeline_id}_job1")
        job2_data = get_job_by_tag(f"{pipeline_id}_job2")
        job3_data = get_job_by_tag(f"{pipeline_id}_job3")

        if not job1_data and not job2_data and not job3_data:
            return jsonify({
                "pipelineId": pipeline_id,
                "status": "INITIALIZING"
            }), 200
        
        #Job1
        p1 = 0.0 
        state1 = "PENDING" 
        if job1_data:
            p1 = float(job1_data.get("progress", 0.0))
            state1 = job1_data.get("state") 
        
        #Job2
        p2 = 0.0
        state2 = "PENDING"
        if job2_data:
            p2 = float(job2_data.get("progress", 0.0))
            state2 = job2_data.get("state")
            
        #Job3
        p3 = 0.0
        state3 = "PENDING"
        if job3_data:
            p3 = float(job3_data.get("progress", 0.0))
            state3 = job3_data.get("state")

        #set pipeline status
        global_status = "RUNNING"
        if "FAILED" in [state1, state2, state3] or "KILLED" in [state1, state2, state3]:
            global_status = "FAILED"
        elif state3 == "FINISHED":
            global_status = "COMPLETED"

        return jsonify({
            "pipelineId": pipeline_id,
            "status": global_status,
            "steps": {
                "job1": {
                    "name": "Job1 - Aggregation",
                    "job_id": job1_data.get("id") if job1_data else None,
                    "state": state1,
                    "progress_percent": p1
                },
                "job2": {
                    "name": "Job2 - Ranking",
                    "job_id": job2_data.get("id") if job2_data else None,
                    "state": state2,
                    "progress_percent": p2
                },
                "job3": {
                    "name": "Job3 - Anomaly",
                    "job_id": job3_data.get("id") if job3_data else None,
                    "state": state3,
                    "progress_percent": p3
                }
            }
        }), 200

    except Exception as e:
        return jsonify({'error': f'Error computing pipeline status: {str(e)}'}), 500
    

#GET Endpoint to show all the jobs in Hadoop
@app.route('/jobs/hadoop', methods=['GET'])
def list_jobs():
    yarn_url = f"http://{hostname}:8088/ws/v1/cluster/apps?applicationTypes=MAPREDUCE"
    
    try:
        response = requests.get(yarn_url, timeout=10)
        response.raise_for_status()
        json_data = response.json()
        
        apps_container = json_data.get("apps")
        if not apps_container:
            return jsonify([]), 200  #return an empty list if the cluster has not a job
            
        apps_list = apps_container.get("app", [])
            
        return apps_list, 200

    except requests.exceptions.RequestException as e:
        return jsonify({'error': f'Connection error to YARN: {e}'}), 500


#GET Endpoint to return the result of a job in Hadoop
@app.route('/jobs/hadoop/<job_id>/result', methods=['GET'])
def get_job_result(job_id):
    yarn_url = f"http://{hostname}:8088/ws/v1/cluster/apps/{job_id}"
    response_yarn = requests.get(yarn_url, timeout=10)
    
    #if YARN responds with 404, the job does not exist
    if response_yarn.status_code == 404:
        return jsonify({'error': 'Job not found on YARN'}), 404
        
    response_yarn.raise_for_status()
    
    #extract the "app" object from the JSON
    data_app = response_yarn.json().get('app', {})
    
    status = data_app.get('state')
    if status != 'FINISHED':
        return jsonify({'error': f'Result not available. Job status: {status}'}), 400
        
    #extract the tag (which acts as the folder name)
    job_tag = data_app.get('applicationTags')
    if not job_tag:
        return jsonify({'error': 'This job has not a tag associated to find output folder'}), 400
    
    all_content = b""
    
    for i in range(10):  
        part_name = f"part-r-{str(i).zfill(5)}"
        webhdfs_url = f"http://{hostname}:9870/webhdfs/v1/user/hadoop/{job_tag}/{part_name}?op=OPEN" #call to the NameNode to obtain the result 

        try:
            #set allow_redirects=False to block automatic redirects
            response_namenode = requests.get(webhdfs_url, allow_redirects=False, timeout=10)
            
            #check if it responded with a Redirect 
            if response_namenode.status_code == 307:
                #extract the "wrong" URL from the datanode
                redirect_url = response_namenode.headers['Location']
                
                #replace hostnames impossible to resolve with our VM's IP
                correct_redirect_url = redirect_url.replace('hadoop-datanode-3', '10.1.1.143')
                correct_redirect_url = correct_redirect_url.replace('hadoop-datanode-2', '10.1.1.111')
                correct_redirect_url = correct_redirect_url.replace('hadoop-namenode', '10.1.1.124')
                
                #execute redirected request
                final_response = requests.get(correct_redirect_url, stream=True, timeout=30) 
                final_response.raise_for_status()
                all_content += final_response.content 

            elif response_namenode.status_code == 404: 
                break  
                
        except requests.exceptions.RequestException:
            break

    if not all_content: 
        return jsonify({'error': 'No result files found'}), 404
    
    return Response(
        all_content,
        content_type='text/plain',
        headers={"Content-Disposition": f"attachment; filename=result_{job_id}.txt"}
    )


#GET Endpoint to monitor the status of a single Job (both for Hadoop and Spark)
@app.route('/jobs/<job_id>', methods=['GET'])
def get_job(job_id):
    yarn_url = f"http://{hostname}:8088/ws/v1/cluster/apps/{job_id}"
    
    try:
        response = requests.get(yarn_url, timeout=10)
        response.raise_for_status()
        json_data = response.json()
            
        app_yarn = json_data.get("app", [])
        if not app_yarn:
            return jsonify({'error': f'Job with tag {job_id} not found'}), 404
        
        return app_yarn, 200

    except requests.exceptions.RequestException as e:
        return jsonify({'error': f'Connection error to YARN: {e}'}), 500


#Spark


#POST Endpoint to start job in Spark
@app.route('/jobs/spark', methods=['POST'])
def create_spark_job():
    data = request.get_json() or {} 

    dataset = data.get('dataset')

    if not dataset:
        return jsonify({
            "error": "Bad request",
            "message": "Dataset is required"
        }), 400

    #job parameters
    min_reviews_perfect = data.get('minReviewsPerfect', 100)
    min_helpful_ratio = data.get('minHelpfulRatio', 0.2)
    min_verified_ratio = data.get('minVerifiedRatio', 0.5)

    #unique tag to identify the job
    job_tag = str(uuid.uuid4())
    
    #output folders
    output_ranking = f"{job_tag}_ranking"
    output_anomalies = f"{job_tag}_anomalies"

    #command spark-submit via SSH
    command = (
        #need to set hadoop bin path, because spark-submit internally uses hadoop
        f"export PATH=$PATH:/opt/hadoop/bin && "
        #HADOOP_CONF_DIR contains path of configuration files for hadoop
        f"export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop && "
        f"nohup /opt/spark/bin/spark-submit "
        f"--master yarn " #use yarn to manage resources
        f"--deploy-mode cluster "  #driver runs on a node of the cluster
        f"--conf spark.yarn.tags=\"{job_tag}\" " #assign a unique tag to the yarn job
        f"/home/hadoop/amazon_project/spark/amazon_spark.py "
        f"{dataset} {output_ranking} {output_anomalies} "
        f"{min_reviews_perfect} {min_helpful_ratio} {min_verified_ratio} "
        f"> /home/hadoop/spark_{job_tag}.log 2>&1 < /dev/null &"
    )

    execute_ssh_command(hostname, username, password, command)

    max_retries = 20
    seconds_to_wait = 3

    for retry in range(max_retries):
        try:
            app_yarn = get_job_by_tag(job_tag)
            if app_yarn is not None:
                return jsonify({
                    "message": "Spark Job started successfully",
                    "jobId": app_yarn.get("id"),
                    "jobTag": job_tag,
                    "jobStatus": app_yarn.get("state"),
                    "outputRankingFolder": output_ranking,
                    "outputAnomaliesFolder": output_anomalies,
                    "minReviewsPerfect": min_reviews_perfect,
                    "minHelpfulRatio": min_helpful_ratio,
                    "minVerifiedRatio": min_verified_ratio
                }), 201
            print(f"Retry {retry + 1}/{max_retries}: Spark job not yet on YARN. Waiting...")
            time.sleep(seconds_to_wait)

        except requests.exceptions.RequestException as e:
            print(f"Temporary error on YARN: {e}")
            time.sleep(seconds_to_wait)

    return jsonify({
        "error": "Timeout",
        "message": "Command has been sent, but the job seems not to be started",
        "job_tag": job_tag
    }), 500 


# GET Endpoint to show all the jobs in Spark
@app.route('/jobs/spark', methods=['GET'])
def list_spark_jobs():
    yarn_url = f"http://{hostname}:8088/ws/v1/cluster/apps?applicationTypes=SPARK"
    
    try:
        response = requests.get(yarn_url, timeout=10)
        response.raise_for_status()
        json_data = response.json()
        
        apps_container = json_data.get("apps")
        if not apps_container:
            return jsonify([]), 200  #return an empty list if the cluster has not a job
            
        apps_list = apps_container.get("app", [])
        
        return apps_list, 200

    except requests.exceptions.RequestException as e:
        return jsonify({'error': f'Connection error to YARN: {e}'}), 500
    

#GET Endpoint to return the results of a job in Spark (ranking or anomalies)
@app.route('/jobs/spark/<job_id>/result/<output_type>', methods=['GET'])
def get_spark_job_result(job_id, output_type):
    #spark produces two distinct outputs: 'ranking' or 'anomalies'
    if output_type not in ['ranking', 'anomalies']:
        return jsonify({'error': 'invalid output_type. Use "ranking" or "anomalies"'}), 400

    yarn_url = f"http://{hostname}:8088/ws/v1/cluster/apps/{job_id}"
    response_yarn = requests.get(yarn_url, timeout=10)
    
    if response_yarn.status_code == 404:
        return jsonify({'error': 'Spark Job not found in YARN'}), 404
        
    response_yarn.raise_for_status()
    data_app = response_yarn.json().get('app', {})
    
    status = data_app.get('state')
    if status != 'FINISHED':
        return jsonify({'error': f'Result not available. Status: {status}'}), 400
        
    job_tag = data_app.get('applicationTags')
    if not job_tag:
        return jsonify({'error': 'No tag associated with this Job.'}), 400
    
    #spark file output name = part-00000
    target_folder = f"{job_tag}_{output_type}"
    all_content = b""

    for i in range(10):
        part_name = f"part-{str(i).zfill(5)}"
        webhdfs_url = f"http://{hostname}:9870/webhdfs/v1/user/hadoop/{target_folder}/{part_name}?op=OPEN"
        
        try:
            response_namenode = requests.get(webhdfs_url, allow_redirects=False, timeout=10)
            
            if response_namenode.status_code == 307:
                redirect_url = response_namenode.headers['Location']
                
                correct_redirect_url = redirect_url.replace('hadoop-datanode-3', '10.1.1.143')
                correct_redirect_url = correct_redirect_url.replace('hadoop-datanode-2', '10.1.1.111')
                correct_redirect_url = correct_redirect_url.replace('hadoop-namenode', '10.1.1.124')
                
                final_response = requests.get(correct_redirect_url, stream=True, timeout=30)
                final_response.raise_for_status()
                all_content += final_response.content

            elif response_namenode.status_code == 404:
                break  

        except requests.exceptions.RequestException:
            break

    if not all_content:
        return jsonify({'error': 'No result files found'}), 404

    return Response(
        all_content,
        content_type='text/plain',
        headers={"Content-Disposition": f"attachment; filename={output_type}_{job_id}.txt"}
    )

#html page
@app.route('/')
def home():
    return send_file('index.html')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)