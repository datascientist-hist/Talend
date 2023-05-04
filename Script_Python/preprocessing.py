## --------------------
## Caricamento Librerie
## --------------------
import pandas as pd
import re
import os
from os import listdir
from os.path import isfile, join
import time
from datetime import datetime, date
import shutil
import sys
import numpy as np
import math


## ------------------------
## Caricamento Path da Bash
## ------------------------

#path="C:/Users/pepee/Desktop/Tirocinio/Talend/Dati_Giornalieri/"
path= sys.argv[1]
path_out=sys.argv[2]
path_out_pluvio=path_out+"/Pluvio/"
path_out_termo=path_out+"/Termo/"

## -------------------------------------------------------------
## Creazione lista file presenti nella cartella Dati_Giornalieri 
## -------------------------------------------------------------

files = [f for f in listdir(path) if isfile(join(path, f)) ]

## -------------------------------------------------------------
## Verifica Correttezza Nome File 
## DatiPluvio_2015-01-01_00-00.csv
## DatiTermo_2015-01-01_00-00.csv
## Se presenti errori sposto file nella cartella Errore_Nome_File
## -------------------------------------------------------------
pluvio=[]
termo=[]
copy_files=files.copy()
for file in copy_files:
    pluvio_check=re.findall("DatiPluvio_\\d{4}\\-\\d{2}\\-\\d{2}\\_\\d{2}\\-\\d{2}\\.csv",file)
    termo_check=re.findall("DatiTermo_\\d{4}\\-\\d{2}\\-\\d{2}\\_\\d{2}\\-\\d{2}\\.csv",file)

    if(len(pluvio_check)>0):
        pluvio.append(pluvio_check[0])
        files.remove(file)
        

    elif(len(termo_check)>0):
        termo.append(termo_check[0])
        files.remove(file)
        
    else:
        
        path_error_name_file=path_out+"/Errore_Nome_File/"
        old_file = os.path.join(path,file)
        new_file = os.path.join(path_error_name_file, (file))
        shutil.copy2(old_file, new_file)
        os.remove(old_file)
        files.remove(file)   
        

## ------------------------------------------------------------------
## Verifica Intestazione File Pluvio 
## Se presenti errori sposto i files nella cartella: Errore_Header
## ------------------------------------------------------------------
# Pluvio
path_error_header=path_out+"/Errore_Header/"
path_error_values=path_out+"/Errore_Values/"
copy_pluvio=pluvio.copy()
for file in pluvio:
    flag=False
    full_path=join(path,file)
    df=pd.read_csv(full_path)

    ## CHECK HEADER 1
    ## Checking if this first row contains numbers and delete all the space in the header
    try:
        l= [int(x) for x in df.columns]
        flag=True
    except:
        flag=False
    if(flag == False):
        
        new_col=[]
        for i in range(0 ,len(df.columns)):
            p=re.sub(r'[^a-zA-Z0-9]', '', df.columns[i])
            new_col.append(p)
        df.columns=new_col       

    ## CHECK HEADER 2 
    ## Checking if the second header  contains the same city as the previous row    
    if(flag==False):
        
        if(df.iloc[0][0]=='Location Ids'):
            
            for col in df.iloc[0][1:]:
                col_mf=re.sub(r'[^a-zA-Z0-9]', '', col)  
                
                if col_mf not in df.columns:
                    flag=True
        else:
            flag=True
                
    ## CHECK HEADER 3
     ## Checking if the third header contains the following pattern- Time|Rainfall|Rainfall|...        
    if(flag==False):
        
        if(df.iloc[1][0]=='Time'):
            for col in df.iloc[1][1:]:
                if not (col=='Rainfall'):
                    flag=True
        else:
            flag=True
                
    ## SE ERRORI
    ## Sposto i file nella cartella errore header se sono stati trovati errori nella formattazione dell'header
    if(flag== True):
        old_file = os.path.join(path,file)
        new_file = os.path.join(path_error_header,(file))
        shutil.copy2(old_file, new_file)
        os.remove(old_file)
        copy_pluvio.remove(file)
        flag=False
        continue
        
    
    ## CHANGE SCHEMA
    df.rename(columns = {'Unnamed0':'Date'}, inplace = True)
    
    ## REMOVING ROWS HEADER
    df=df.iloc[2:]
    ## CHANGE SCHEMA
    try:
         ## CHECKING Number COLUMNS
        df[df.columns[1:]] = df[df.columns[1:]].apply(pd.to_numeric)
         ## CHECKING DateTime COLUMNS
        df['Date'] =  pd.to_datetime(df['Date'], format='%Y-%m-%d %H:%M:%S.%f')
    except:
        flag:True
        
    ## CHECKING RANGE VALUES
    tot=[]
    if(flag==False):
        for col in df.columns[1:]:
            series=df[col].loc[lambda x : (x >= 0)]
            diff=series.max()-series.min()
           
            if not ((diff >=0 and diff<=200) or math.isnan(diff)):
                flag=True

    ## Se ci sono errori nel range dei valori o nella colonna date
    ## sposto i file nella cartella Errore_Values
    if(flag==True):
        old_file = os.path.join(path,file)
        new_file = os.path.join(path_error_values,file)
        shutil.copy2(old_file, new_file)
        os.remove(old_file)
        copy_pluvio.remove(file)
        continue

    ## Se il file ha passato i controlli converto lo schema da tre header a uno solo
    ## con il seguente schema ['Date','Rainfall','Station']  
    if(flag == False):
        dataf=pd.DataFrame(columns=['Date','Rainfall','Station'])
        for col in df.columns[1:]:
            new= pd.DataFrame(df[["Date",col]])
            new["Station"]=col
            new.columns=['Date','Rainfall','Station']
            dataf=pd.concat([dataf, new])
        ## sposto il file nella cartella pluvio    
        final_path=join(path_out_pluvio,file)
        dataf.to_csv(final_path, index=False)
        old_file = os.path.join(path,file)
        os.remove(old_file)
        copy_pluvio.remove(file)


## ------------------------------------------------------------------
## Verifica Intestazione File Termo 
## Se presenti errori sposto i files nella cartella: Errore_Header
## ------------------------------------------------------------------
# Pluvio
copy_termo=termo.copy()
for file in termo:
    flag=False
    full_path=join(path,file)
    df=pd.read_csv(full_path)

    ## CHECK HEADER 1
    ## Checking if this first row contains numbers and delete all the space in the header
    try:
        l= [int(x) for x in df.columns]
        flag=True
    except:
        flag=False
    if(flag == False):
        
        new_col=[]
        for i in range(0 ,len(df.columns)):
            p=re.sub(r'[^a-zA-Z0-9]', '', df.columns[i])
            new_col.append(p)
        df.columns=new_col       

    ## CHECK HEADER 2 
    ## Checking if the second header  contains the same city as the previous row    
    if(flag==False):
        
        if(df.iloc[0][0]=='Location Ids'):
            
            for col in df.iloc[0][1:]:
                col_mf=re.sub(r'[^a-zA-Z0-9]', '', col)  
                
                if col_mf not in df.columns:
                    flag=True
        else:
            flag=True
                
    ## CHECK HEADER 3
     ## Checking if the third header contains the following pattern- Time|Rainfall|Rainfall|...        
    if(flag==False):
        
        if(df.iloc[1][0]=='Time'):
            for col in df.iloc[1][1:]:
                if not (col=='Temperature'):
                    flag=True
        else:
            flag=True
                
    ## SE ERRORI
    ## Sposto i file nella cartella errore header se sono stati trovati errori nella formattazione dell'header
    if(flag== True):
        old_file = os.path.join(path,file)
        new_file = os.path.join(path_error_header,(file))
        shutil.copy2(old_file, new_file)
        os.remove(old_file)
        copy_termo.remove(file)
        flag=False
        continue
        
    
    ## CHANGE SCHEMA
    df.rename(columns = {'Unnamed0':'Date'}, inplace = True)
    
    ## REMOVING ROWS HEADER
    df=df.iloc[2:]
    ## CHANGE SCHEMA
    try:
         ## CHECKING Number COLUMNS
        df[df.columns[1:]] = df[df.columns[1:]].apply(pd.to_numeric)
         ## CHECKING DateTime COLUMNS
        df['Date'] =  pd.to_datetime(df['Date'], format='%Y-%m-%d %H:%M:%S.%f')
    except:
        flag:True
        
    ## CHECKING RANGE VALUES

    if(flag==False):
        for col in df.columns[1:]:
            series=df[col].loc[lambda x : (x >= 0) ]
            diff=series.max()-series.min()
            if not ((diff >=0 and diff<=200) or math.isnan(diff)):
                flag=True
    ## Se ci sono errori nel range dei valori o nella colonna date
    ## sposto i file nella cartella Errore_Values
    if(flag==True):
        old_file = os.path.join(path,file)
        new_file = os.path.join(path_error_values,file)
        shutil.copy2(old_file, new_file)
        os.remove(old_file)
        copy_termo.remove(file)
        continue

    ## Se il file ha passato i controlli converto lo schema da tre header a uno solo
    ## con il seguente schema ['Date','Rainfall','Station']  
    if(flag == False):
        dataf=pd.DataFrame(columns=['Date','Temperature','Station'])
        for col in df.columns[1:]:
            new= pd.DataFrame(df[["Date",col]])
            new["Station"]=col
            new.columns=['Date','Temperature','Station']
            dataf=pd.concat([dataf, new])
        ## sposto il file nella cartella pluvio    
        final_path=join(path_out_termo,file)
        dataf.to_csv(final_path, index=False)
        old_file = os.path.join(path,file)
        os.remove(old_file)
        copy_termo.remove(file)