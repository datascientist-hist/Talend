# this import is used to instantiate dag
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.http_operator import SimpleHttpOperator
from datetime import datetime, timedelta

# this is the arguments used by the bash operator
default_args = {
    'owner': 'Giuseppe_Pulino',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 3),
    'retries': 1,
    'retry_delay': timedelta(minutes=30),
    'catchup': False
}


# Check Response Rest Api Service
def check(response):
    if response == 200:
        print("Returning True")
        return True
    else:
        print("Returning False")
        return False

#################################
#  Defining DAG
#################################    
with DAG(
    dag_id='Talend_Orchestration_V4',
    default_args=default_args,
    access_control={
         'Admin':{'can_dag_read','can_dag_edit'}},
         schedule_interval="@once"
    ) as dag:
        
        ###############################################################
        # First Task Ingest Data using a Python Script executed by bash
        #
        # Files are moved from folder Dati Etl to Dati Giornalieri
        ###############################################################
        python_ingestion=dag.folder+"/JobTalend/Script_Python/ingestion.py"
        input_path_ingestion=dag.folder+"/JobTalend/Dati/datiETL"
        output_path_ingestion=dag.folder+"/JobTalend/Dati/Dati_Giornalieri/"

        Ingestion = BashOperator(
             task_id='Ingestion',
             bash_command="python "+python_ingestion+" "+input_path_ingestion+" "+output_path_ingestion,
             cwd=dag.folder)
        

        
        #########################################################################
        # Second Task Check Integrity Data using a Python Script executed by bash
        #
        # Files are moved from folder Dati Giornalieri to Dati Giornalieri Output
        # if there aren't problems, files are located in the folders Pluvio and Termo
        #########################################################################
        python_validation=dag.folder+"/JobTalend/Script_Python/preprocessing.py"
        input_path_validation=output_path_ingestion
        output_path_validation=dag.folder+"/JobTalend/Dati/Dati_Giornalieri_Output"

        Validation = BashOperator(
                task_id='Validazione',
                bash_command="python "+python_validation+" "+input_path_validation+" "+output_path_validation,
                cwd=dag.folder
        )

        #########################################################################
        # Third Task (A) Call the Rest Api Service that starts the uploading on 
        # MongoDB of the Pluvio Data
        #
        # Files are moved from folder Pluvio to Pluvio_Raw collection on MongoDB 
        # and then are deleted
        #########################################################################

        command_Api ="activate/C:--Users--pepee--Desktop--Tirocinio--Airflow--dags--JobTalend--Dati--Dati_Giornalieri_Output--Pluvio"
        request_Api = SimpleHttpOperator(
            task_id='Start_Service_Data_Pluvio_to_Mongo',
            http_conn_id='web_api',
            method='GET',
            endpoint=command_Api,
            response_check=lambda response: True if check(response.status_code) is True else False,
            )
        

        #########################################################################
        # Third Task (B) Check the status of the Rest Api Service when it results
        # 'finished' goes forward
        #########################################################################
        check_status_service = HttpSensor(
            
            task_id="Check_Status_Service",
            http_conn_id="web_api",
            method="GET",
            endpoint="status/12",
            response_check=lambda response: "finished" in response.text,
            poke_interval=15
            
            )
        
        #########################################################################
        # Fourth Task upload to MongoDB the Termo Data executing  a bash script
        # produced by Talend, the script inside will execute a java script
        #
        # Files are moved from folder Termo to Termo_Raw Collection on MongoDB 
        # and then are deleted
        #########################################################################

        bash_command_termo=dag.folder+"/JobTalend/Talend/Termo_MDB/Caricamento_MDB_Termo/Caricamento_MDB_Termo_run.sh "
        context_param_termo ="--context_param termo="+output_path_validation+"/Termo/"
        Dati_Termo_to_MongoDB = BashOperator(
                task_id='Dati_Termo_to_MongoDB',
                bash_command=bash_command_termo+context_param_termo,
                cwd=dag.folder
        )

        #########################################################################
        # Fifth Task  download Pluvio Raw Data from MongoDB, it corrects them,
        # it adds new information like zone and then upload these Data into Pluvio 
        # table on MySql Database
        #
        # This task execute a bash script produced by Talend, the script inside 
        # will execute a java script
        #########################################################################
        bash_command_pro_pluvio=dag.folder+"/JobTalend/Talend/Data_Pro_Pluvio/Data_Processing_Pluvio/Data_processing_Pluvio_run.sh "
        context_param_pro_pluvio ="--context_param arr_pluvio="+output_path_validation+"/Lookup_Pluvio/lookup_pluvio.csv"
        Data_Processing_Pluvio = BashOperator(
                task_id='Data_Processing_Pluvio',
                bash_command=bash_command_pro_pluvio+context_param_pro_pluvio,
                cwd=dag.folder
        )

        #########################################################################
        # Sixth Task  download Termo Raw Data from MongoDB, it corrects them,
        # it adds new information like zone and then upload these Data into Termo 
        # table on MySql Database
        #
        # This task execute a bash script produced by Talend, the script inside 
        # will execute a java script
        #########################################################################

        bash_command_pro_termo=dag.folder+"/JobTalend/Talend/Data_Pro_Termo/Data_Processing_Termo/Data_processing_Termo_run.sh "
        context_param_pro_termo ="--context_param arr_termo="+output_path_validation+"/Lookup_Termo/lookup_termo.csv"
        Data_Processing_Termo = BashOperator(
                task_id='Data_Processing_Termo',
                bash_command=bash_command_pro_termo+context_param_pro_termo,
                cwd=dag.folder
        )

        #########################################################################
        # Seventh Task  executing a bash script produced by Talend, creates aggregate 
        # Pluvio Data starting from MySql table and stores this new data into new tables 
        # on MySQL
        #
        #########################################################################

        bash_command_query_pluvio=dag.folder+"/JobTalend/Talend/Query_Pluvio/Query_Pluvio/Query_Pluvio_run.sh "
        Data_Query_Pluvio = BashOperator(
                task_id='Data_Query_Pluvio',
                bash_command=bash_command_query_pluvio,
                cwd=dag.folder
        )

        #########################################################################
        # Eighth Task  executing a bash script produced by Talend, creates aggregate 
        # Termo Data starting from MySql table and stores this new data into new tables 
        # on MySQL
        #
        #########################################################################

        bash_command_query_termo=dag.folder+"/JobTalend/Talend/Query_Termo/Query_Termo/Query_Termo_run.sh "
        Data_Query_Termo = BashOperator(
                task_id='Data_Query_Termo',
                bash_command=bash_command_query_termo,
                cwd=dag.folder
        )

        #########################################################################
        # Ninth Task wait the end of the other tasks and then send an email to notify 
        # the end of the pipeline
        #
        #########################################################################
        email = EmailOperator(
            task_id='Send_Email_End_Report',
            to='pulino918@gmail.com',
            subject='Airflow Alert',
            html_content=""" <h1>Pipeline Completata</h1> """
            )
        
        #  Schema Pipeline Pluvio/Termo
        #         
        #                           request_Api -> check_status_service -> Data_Processing_Pluvio -> Data_Query_Pluvio
        # 
        # Ingestion -> Validation ->                                                                                    -> email
        # 
        #                           Dati_Termo_to_MongoDB ->               Data_Processing_Termo ->  Data_Query_Termo
                    
        Ingestion>>Validation
        Validation>>[request_Api,Dati_Termo_to_MongoDB]
        request_Api>>check_status_service
        check_status_service>>Data_Processing_Pluvio #ok
        Dati_Termo_to_MongoDB>>Data_Processing_Termo #ok
        Data_Processing_Pluvio>>Data_Query_Pluvio
        Data_Processing_Termo>>Data_Query_Termo
        [Data_Query_Pluvio,Data_Query_Termo]>>email
        
