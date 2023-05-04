# this import is used to instantiate dag
from airflow import DAG
# this import is used to run the tasks
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
#Passing the sh file

# this is the arguments used by the bash operator
default_args = {
    'owner': 'abc',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 3),
    'retries': 1,
    'retry_delay': timedelta(minutes=30),
    'catchup': False
}
# instantiate a DAG

with DAG(
    dag_id='Talend_Orchestration_V4',
    default_args=default_args,
    access_control={'Admin':{'can_dag_read','can_dag_edit'}},schedule_interval="@once"
    ) as dag:
        
        python_ing=dag.folder+"/JobTalend/Script_Python/ingestion.py"
        input_path_ing=dag.folder+"/JobTalend/Dati/datiETL"
        output_path_ing=dag.folder+"/JobTalend/Dati/Dati_Giornalieri/"
        #task are given some specific arguments we can also override the default_args here.
        t1 = BashOperator(
             task_id='Ingestion',
             bash_command="python "+python_ing+" "+input_path_ing+" "+output_path_ing,
             cwd=dag.folder)
        
        python_val=dag.folder+"/JobTalend/Script_Python/preprocessing.py"
        input_path_val=output_path_ing
        output_path_val=dag.folder+"/JobTalend/Dati/Dati_Giornalieri_Output"
        t2 = BashOperator(
                task_id='Validazione',
                bash_command="python "+python_val+" "+input_path_val+" "+output_path_val,
                cwd=dag.folder
        )

        bash_command_pl=dag.folder+"/JobTalend/Talend/Pluvio_MDB/Caricamento_MDB_Pluvio/Caricamento_MDB_Pluvio_run.sh "
        context_param_pl ="--context_param pluvio="+output_path_val+"/Pluvio/"
        t3 = BashOperator(
                task_id='Dati_Pluvio_to_MongoDB' ,
                bash_command=bash_command_pl+context_param_pl,
                cwd=dag.folder
        )

        bash_command_trm=dag.folder+"/JobTalend/Talend/Termo_MDB/Caricamento_MDB_Termo/Caricamento_MDB_Termo_run.sh "
        context_param_trm ="--context_param termo="+output_path_val+"/Termo/"
        t4 = BashOperator(
                task_id='Dati_Termo_to_MongoDB',
                bash_command=bash_command_trm+context_param_trm,
                cwd=dag.folder
        )

        bash_command_pro_pluvio=dag.folder+"/JobTalend/Talend/Data_Pro_Pluvio/Data_Processing_Pluvio/Data_processing_Pluvio_run.sh "
        context_param_pro_pluvio ="--context_param arr_pluvio="+output_path_val+"/Lookup_Pluvio/lookup_pluvio.csv"
        t5 = BashOperator(
                task_id='Data_Processing_Pluvio',
                bash_command=bash_command_pro_pluvio+context_param_pro_pluvio,
                cwd=dag.folder
        )

        bash_command_pro_termo=dag.folder+"/JobTalend/Talend/Data_Pro_Termo/Data_Processing_Termo/Data_processing_Termo_run.sh "
        context_param_pro_termo ="--context_param arr_termo="+output_path_val+"/Lookup_Termo/lookup_termo.csv"
        t6 = BashOperator(
                task_id='Data_Processing_Termo',
                bash_command=bash_command_pro_termo+context_param_pro_termo,
                cwd=dag.folder
        )

        bash_command_query_pluvio=dag.folder+"/JobTalend/Talend/Query_Pluvio/Query_Pluvio/Query_Pluvio_run.sh "
        t7 = BashOperator(
                task_id='Data_Query_Pluvio',
                bash_command=bash_command_query_pluvio,
                cwd=dag.folder
        )

        bash_command_query_termo=dag.folder+"/JobTalend/Talend/Query_Termo/Query_Termo/Query_Termo_run.sh "
        t8 = BashOperator(
                task_id='Data_Query_Termo',
                bash_command=bash_command_query_termo,
                cwd=dag.folder
        )
                  
                     
        t1>>t2
        t2>>[t3,t4]
        t3>>t5 #ok
        t4>>t6 #ok
        t5>>t7
        t6>>t8
