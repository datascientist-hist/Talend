import pandas as pd
import re
import os
from os import listdir
from os.path import isfile, join
import time
from datetime import datetime, date
import shutil
import pickle


path_in_pluvio="C:/Users/pepee/Desktop/Tirocinio/Talend/Dati_Giornalieri_Output/Correzzione/Pluvio/"
path_in_termo="C:/Users/pepee/Desktop/Tirocinio/Talend/Dati_Giornalieri_Output/Correzzione/Termo/"

path_out_pluvio="C:/Users/pepee/Desktop/Tirocinio/Talend/Dati_Giornalieri_Output/Corretti/Pluvio"
path_out_termo="C:/Users/pepee/Desktop/Tirocinio/Talend/Dati_Giornalieri_Output/Corretti/Termo"

files_pluvio = [f for f in listdir(path_in_pluvio) if isfile(join(path_in_pluvio, f)) ]
files_termo  = [f for f in listdir(path_in_termo) if isfile(join(path_in_termo, f)) ]

if(len(files_pluvio)>0):
    df=pd.read_csv(join(path_in_pluvio,files_pluvio[0]))
    path_pickle_pluvio="C:/Users/pepee/Desktop/Tirocinio/Talend/Dati_Giornalieri_Output/Correzzione/Pluvio/tmp/tmp.p"

    try:
        diict_value_pluvio= pickle.load(open(path_pickle_pluvio, "rb"))
    
    except:
        diict_value_pluvio={}

    try:
        df['Date'] =  pd.to_datetime(df['Date'], format='%Y-%m-%d %H:%M:%S.%f')

    except:
        print('error datetime')
    df.sort_values(by='Date', ascending = True, inplace = True)
    df.reset_index(drop=True,inplace=True)

    for idx in range(len(df)):
        value=df.iloc[idx][1]
        city=df.iloc[idx][2]
        if(value>=0 and value <=200):
            diict_value_pluvio[city]=value
        else:
            try:
                old_value=diict_value_pluvio[city]
                df.at[idx,'Rainfall']=old_value
            except:
                df.at[idx,'Rainfall']= -50.00


    pickle.dump(diict_value_pluvio, open(path_pickle_pluvio, "wb")) 
    final_path=join(path_out_pluvio,files_pluvio[0])
    df.to_csv(final_path, index=False)
    old_file = os.path.join(path_in_pluvio,files_pluvio[0])
    os.remove(old_file)

    # LOOKUP TABLE
    lookup_df=pd.DataFrame(df['Station'].unique(),columns=["Station"])
    import string

    alph_ls=list(string.ascii_uppercase)
    split_df=int(len(alph_ls)/10)
    dict_alph={}
    for idx in range(len(alph_ls)):
        div=idx//split_df
        if(div<10):
            dict_alph[alph_ls[idx]]="Zone "+str(div+1)
        else:
            dict_alph[alph_ls[idx]]="Zone 10"
    zone_ls=[]
    for idx in range(len(lookup_df)):
        name=lookup_df['Station'].iloc[idx][0]
        name=name.upper()
        zone_ls.append(dict_alph[name])
    lookup_df['Zone']=zone_ls
    lk_path="C:/Users/pepee/Desktop/Tirocinio/Talend/Dati_Giornalieri_Output/Corretti/Pluvio/Lookup/lookup_pluvio.csv"
    lookup_df.to_csv(lk_path,index=False)
    

if(len(files_termo)>0):    
    df=pd.read_csv(join(path_in_termo,files_termo[0]))
    path_pickle_termo="C:/Users/pepee/Desktop/Tirocinio/Talend/Dati_Giornalieri_Output/Correzzione/Termo/tmp/tmp.p"

    try:
        diict_value_termo= pickle.load(open(path_pickle_termo, "rb"))
    
    except:
        diict_value_termo={}

    try:
        df['Date'] =  pd.to_datetime(df['Date'], format='%Y-%m-%d %H:%M:%S.%f')

    except:
        print('error datetime')
    df.sort_values(by='Date', ascending = True, inplace = True)
    df.reset_index(drop=True,inplace=True)

    for idx in range(len(df)):
        value=df.iloc[idx][1]
        city=df.iloc[idx][2]
        if(value>= -30 and value <=70):
            diict_value_termo[city]=value
        else:
            try:
                old_value=diict_value_termo[city]
                df.at[idx,'Temperature']=old_value
            except:
                df.at[idx,'Temperature']= -50.00


    pickle.dump(diict_value_termo, open(path_pickle_termo, "wb")) 
    final_path=join(path_out_termo,files_termo[0])
    df.to_csv(final_path, index=False)
    old_file = os.path.join(path_in_termo,files_termo[0])
    os.remove(old_file)
    # LOOKUP TABLE
    lookup_df=pd.DataFrame(df['Station'].unique(),columns=["Station"])
    import string

    alph_ls=list(string.ascii_uppercase)
    split_df=int(len(alph_ls)/10)
    dict_alph={}
    for idx in range(len(alph_ls)):
        div=idx//split_df
        if(div<10):
            dict_alph[alph_ls[idx]]="Zone "+str(div+1)
        else:
            dict_alph[alph_ls[idx]]="Zone 10"
    zone_ls=[]
    for idx in range(len(lookup_df)):
        name=lookup_df['Station'].iloc[idx][0]
        name=name.upper()
        zone_ls.append(dict_alph[name])
    lookup_df['Zone']=zone_ls
    lk_path="C:/Users/pepee/Desktop/Tirocinio/Talend/Dati_Giornalieri_Output/Corretti/Termo/Lookup/lookup_termo.csv"
    lookup_df.to_csv(lk_path,index=False)