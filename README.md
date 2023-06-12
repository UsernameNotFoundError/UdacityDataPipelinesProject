# UdacityDataPipelinesProject
Create high grade data pipelines with Airflow. 

## Setup Instructions

Setting up a virtual environment is recommanded to ensure the correct dependencies are installed and isolated from your system's global Python environment. Please follow the instructions below to set up the virtual environment and run the script.

### Prerequisites
* Python **3.7** installed on your system.
* Pip package manager installed.
* **Ubuntu** or WSL installed
* Patience 

### I. Set up the Vitual environment 
**3. Navigate to the project directory (or create one):**
```bash
cd <project_directory>
```
**2. Clone the repository to your local machine using the following command:**
``` cmd
git clone https://github.com/UsernameNotFoundError/UdacityDataPipelinesProject.git
```

**3. Create a virtual environment using the venv module. Run the following command:**

```cmd
python3.7 -m venv venv_name
```
PS: in this example we are using python3.7, feel free to use other version (not recommanded) that are compatible with [airflow](https://airflow.apache.org/docs/apache-airflow/stable/installation/prerequisites.html)

**4. Activate the virtual environment:**
```shell
source venv_name/bin/activate
```
PS: To desactivate the virtual environment use the `deactivate` command.

**5. Install apache airflow and its required dependencies using pip:**

```shell
pip3.7 install "apache-airflow==2.6.1" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.6.1/constraints-3.7.txt"
```
P.S: (Not recommanded) If you're using another python version please refer to the apatche [airflow documentation](https://airflow.apache.org/docs/apache-airflow/stable/start.html). 

**6. Once the dependencies are installed, you can now run the extra requirements:**
```shell
pip3.7 install -r requirements.txt
```

**7. After you have finished working with the project, you can deactivate the virtual environment by running the following command:**
```cmd
deactivate
```

### II. Airflow:
**source:**  [Apache Airflow Documentation](https://airflow.apache.org/docs/apache-airflow/stable/start.html)

**1. Set Airflow Home:**
```shell
export AIRFLOW_HOME=$(pwd)
```
P.S: make sure to check AIRFLOW_HOME each time before running airflow

**2. Initiate a SQLite database for Airflow in the workspace:**
```shell
airflow db init
```

**3. Create user:**
```shell
airflow users create --username admin1 --firstname firstname --lastname lastname --role Admin --email email@example.com
```
You will be asked to created a password (please remember your password or keep it somewhere safe. It will be be needed to log in)

**4. Lunch webserver and the scheduler:**
```shell
airflow scheduler & airflow webserver
```
The web server and scheduler need to be run simultaneously.

## The pipeline:

### Prerequisites
* Create an IAM User in AWS
* Configure Redshift Serverless

**1. Connect Airflow with AWS:
* On the navigation bar go to: Admin > Connections > "+" (found at teh top left)
* Input your credentials and parameters as shown below:

 <p align="center" width="100%"> <img width="100%" src="https://github.com/UsernameNotFoundError/UdacityDataPipelinesProject/blob/master/airflowaws.png"> </p>





