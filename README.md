<h1>Real-Time Weather and Traffic Data Integration and Analysis</h1>

If you are running the pipeline for the first time (if not, refer to the "how to run" instructions below these):
----------------------------------------------------------------------------------------
1. clone the repository<br>
2. make sure you are in the project folder, then run<br>
python -m venv venv<br>
venv\Scripts\activate (WINDOWS)<br> OR:<br>
source venv/bin/activate (MAC)<br>
pip install -r requirements.txt<br>
3. Requirements<br>
make sure to be using java version "17.0.12" 2024-07-16 LTS<br>
(WINDOWS ONLY) Hadoop / winutils (winutils 3.3.5 with Hadoop.dll too) in C:\hadoop\bin<br>
4. create a .env file with contents:<br>
OPENWEATHER_API_KEY=your_key_here<br>
TOMTOM_API_KEY=your_key_here<br>
5. in spark_streaming.py in the part of the code under "Snowflake Configuration", put your own Snowflake credentials<br>
6. additionally, create a file called rsa_key.p8 and copy and paste your private key that connects to snowflake in there
<br>

HOW TO RUN (WINDOWS):
----------------------------------------------------------------------------------------
1. cd into project folder
2. Run:<br>docker-compose up -d
3. (Terminal 1) Open a powershell terminal and run:<br>
    python .\api_ingestion.py 
4. (Terminal 2) Open powershell in administrator mode and run:<br>
    venv\Scripts\Activate.ps1<br>
    $env:HADOOP_HOME="C:\hadoop"<br>
    $env:PATH="$env:HADOOP_HOME\bin;$env:PATH"<br>
    Remove-Item -Recurse -Force /tmp/spark_checkpoint<br>
    python .\spark_streaming.py<br>
<br>

HOW TO RUN (MAC):
----------------------------------------------------------------------------------------
1. cd into project folder
2. Run:<br>docker-compose up -d
3. (Terminal 1) Run:<br>python3 api_ingestion.py
4. (Terminal 2) Run<br>
source venv/bin/activate<br>
rm -rf /tmp/spark_checkpoint<br>
python3 spark_streaming.py<br>
<br>

If you need to reset all the data in the pipeline and database to start from a clean slate:
----------------------------------------------------------------------------------------
stop api_ingestion.py + spark_streaming.py<Br>

Run in project folder:<br>
docker-compose down -v<br>
docker-compose up -d <br>
<br>
Run in snowflake:<br>
TRUNCATE TABLE weather_traffic_comb;<br>
<br>
then just rerun!

