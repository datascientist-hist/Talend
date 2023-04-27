import pandas as pd
import re
import os
from os import listdir
from os.path import isfile, join
import time
from datetime import datetime, date
import shutil

path="C:/Users/pepee/Desktop/Tirocinio/Talend/Dati_Giornalieri/"
path_out_pluvio="C:/Users/pepee/Desktop/Tirocinio/Talend/Dati_Giornalieri_Output/Pluvio/"
path_out_termo="C:/Users/pepee/Desktop/Tirocinio/Talend/Dati_Giornalieri_Output/Termo/"

files = [f for f in listdir(path) if isfile(join(path, f)) ]

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
        
        path_error_name_file="C:/Users/pepee/Desktop/Tirocinio/Talend/Dati_Giornalieri_Output/Errore_Nome_File/"
        old_file = os.path.join(path,file)
        new_file = os.path.join(path_error_name_file, (file))
        shutil.copy2(old_file, new_file)
        os.remove(old_file)
        files.remove(file)   
        
import pickle
path_metadata="C:/Users/pepee/Desktop/Tirocinio/Talend/Metadata/name_city.pick"
#pickle.dump(name_city, open(path_metadata, "wb"))
name_city = pickle.load(open(path_metadata, "rb"))
new_col=[]
for i in range(0 ,len(name_city)):
    p=re.sub(r'[^a-zA-Z0-9]', '', name_city[i])
    new_col.append(p)
name_city=new_col

# Pluvio
city=name_city.copy()
path_error_header="C:/Users/pepee/Desktop/Tirocinio/Talend/Dati_Giornalieri_Output/Errore_Header/"
path_error_values="C:/Users/pepee/Desktop/Tirocinio/Talend/Dati_Giornalieri_Output/Errore_Values/"
copy_pluvio=pluvio.copy()
for file in pluvio:
    flag=False
    city=name_city.copy()
    city.append('Unnamed0')
    full_path=join(path,file)
    df=pd.read_csv(full_path)

    ## CHECK HEADER 1
    ## Delete all the space in the header
    name_columns=df.columns.copy()
    new_col=[]
    for i in range(0 ,len(name_columns)):
        p=re.sub(r'[^a-zA-Z0-9]', '', name_columns[i])
        new_col.append(p)
    name_columns=new_col 
    df.columns=name_columns
    
    for col in name_columns:
        if col in city:
            city.remove(col)
        else:
            flag=True
            
     ## CHECK HEADER 2
    city=name_city.copy()
    city.append('LocationIds')    
    if(flag==False):
        for col in df.iloc[0]:
            col=re.sub(r'[^a-zA-Z0-9]', '', col)
            if col in city:
                city.remove(col)
            else:
                flag=True
                
    ## CHECK HEADER 3
    city=name_city.copy()
    city.append('Time')              
    if(flag==False):
        if(df.iloc[1][0]=='Time'):
            for col in df.iloc[1][1:]:
                if not (col=='Rainfall'):
                    flag=True
        else:
            flag=True
                

    ## SPOSTO FILE alla cartella errore header
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
    df[df.columns[1:]] = df[df.columns[1:]].apply(pd.to_numeric)

    ## CHECKING DateTime COLUMNS
    try:
        df['Date'] =  pd.to_datetime(df['Date'], format='%Y-%m-%d %H:%M:%S.%f')

    except:
        flag=True
        
    ## CHECKING RANGE VALUES
    tot=[]
    import numpy as np
    for col in df.columns[1:]:
        series=df[col].loc[lambda x : (x >= 0) & (x <100)]
        diff=series.max()-series.min()
        tot.append(diff)
    if not (np.nanmean(tot)>=0 and np.nanmean(tot)<50 ):
        flag = True

    ## IF Problem range values put in the folder error
    if(flag==True):
        old_file = os.path.join(path,file)
        new_file = os.path.join(path_error_values,file)
        shutil.copy2(old_file, new_file)
        os.remove(old_file)
        copy_pluvio.remove(file)
        continue

    ## No problem in the right folder
    if(flag == False):
        dataf= pd.DataFrame(df[["Date",df.columns[1]]])
        dataf["Stazion"]=df.columns[1]
        dataf.columns=['Date','Rainfall','Station']
        for col in df.columns[2:]:
            new= pd.DataFrame(df[["Date",col]])
            new["Stazion"]=col
            new.columns=['Date','Rainfall','Station']
            dataf=pd.concat([dataf, new])
            
            
            

        final_path=join(path_out_pluvio,file)
        dataf.to_csv(final_path, index=False)
        old_file = os.path.join(path,file)
        os.remove(old_file)
        copy_pluvio.remove(file)

pluvio=copy_pluvio.copy()
        


    
    ## Termo

# Pluvio
city=name_city.copy()
path_error_header="C:/Users/pepee/Desktop/Tirocinio/Talend/Dati_Giornalieri_Output/Errore_Header/"
path_error_values="C:/Users/pepee/Desktop/Tirocinio/Talend/Dati_Giornalieri_Output/Errore_Values/"
copy_termo=termo.copy()
for file in termo:
    flag=False
    city=name_city.copy()
    city.append('Unnamed0')
    full_path=join(path,file)
    df=pd.read_csv(full_path)

    ## CHECK HEADER 1
    ## Delete all the space in the header
    name_columns=df.columns.copy()
    new_col=[]
    for i in range(0 ,len(name_columns)):
        p=re.sub(r'[^a-zA-Z0-9]', '', name_columns[i])
        new_col.append(p)
    name_columns=new_col 
    df.columns=name_columns

    for col in name_columns:
        if col in city:
            city.remove(col)
        else:
            flag=True
            
     ## CHECK HEADER 2
    city=name_city.copy()
    city.append('LocationIds')    
    if(flag==False):
        for col in df.iloc[0]:

            col=re.sub(r'[^a-zA-Z0-9]', '', col)
            if col in city:
                city.remove(col)
            else:
                flag=True
                
    ## CHECK HEADER 3
    city=name_city.copy()
    city.append('Time')              
    if(flag==False):
        if(df.iloc[1][0]=='Time'):
            for col in df.iloc[1][1:]:
                if not (col=='Temperature'):
                    flag=True
        else:
            flag=True
                

    ## SPOSTO FILE alla cartella errore header
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
    df[df.columns[1:]] = df[df.columns[1:]].apply(pd.to_numeric)

    ## CHECKING DateTime COLUMNS
    try:
        df['Date'] =  pd.to_datetime(df['Date'], format='%Y-%m-%d %H:%M:%S.%f')

    except:
        flag=True
        
    ## CHECKING RANGE VALUES
    tot=[]
    import numpy as np
    for col in df.columns[1:]:
        series=df[col].loc[lambda x : (x >= 0) & (x <100)]
        diff=series.max()-series.min()
        tot.append(diff)
    if not (np.nanmean(tot)>=-30 and np.nanmean(tot)<70 ):
        flag = True

    ## IF Problem range values put in the folder error
    if(flag==True):
        old_file = os.path.join(path,file)
        new_file = os.path.join(path_error_values,file)
        shutil.copy2(old_file, new_file)
        os.remove(old_file)
        copy_termo.remove(file)
        continue

    ## No problem in the right folder
    if(flag == False):
        dataf= pd.DataFrame(df[["Date",df.columns[1]]])
        dataf["Stazion"]=df.columns[1]
        dataf.columns=['Date','Temperature','Station']
        for col in df.columns[2:]:
            new= pd.DataFrame(df[["Date",col]])
            new["Stazion"]=col
            new.columns=['Date','Temperature','Station']
            dataf=pd.concat([dataf, new])
            
            
            

        final_path=join(path_out_termo,file)
        dataf.to_csv(final_path, index=False)
        old_file = os.path.join(path,file)
        os.remove(old_file)
        copy_termo.remove(file)

termo=copy_termo.copy()
        


    
    