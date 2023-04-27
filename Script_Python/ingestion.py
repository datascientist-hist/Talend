import pandas as pd
import os
from os import listdir
from os.path import isfile, join
import time
from datetime import datetime, date
import shutil

mypath_pluvio = "C:/Users/pepee/Desktop/Tirocinio/Talend/DatiETL/Pluvio/"
mypath_termo= "C:/Users/pepee/Desktop/Tirocinio/Talend/DatiETL/Termo/"

onlyfiles_pluvio = [f for f in listdir(mypath_pluvio) if isfile(join(mypath_pluvio, f)) and f.find(".uploaded") == -1]
onlyfiles_termo= [f for f in listdir(mypath_termo) if isfile(join(mypath_termo, f)) and f.find(".uploaded") == -1]

header_pluvio="DatiPluvio_" #start at 11-21
mod_files_pluvio=[]
for file in onlyfiles_pluvio:
    mod_files_pluvio.append(file[11:21])

mod_files_pluvio
mod_files_pluvio.sort(key=lambda x: datetime.strptime(x, '%Y-%m-%d'),reverse=False)


header_termo="DatiTermo_" #start at 10-20
mod_files_termo=[]
for file in onlyfiles_termo:
    mod_files_termo.append(file[10:20])

mod_files_termo
mod_files_termo.sort(key=lambda x: datetime.strptime(x, '%Y-%m-%d'),reverse=False)

new_path_pluvio="C:/Users/pepee/Desktop/Tirocinio/Talend/Dati_Giornalieri/"
new_path_termo="C:/Users/pepee/Desktop/Tirocinio/Talend/Dati_Giornalieri/"


for file in onlyfiles_pluvio:
    if(file.find(mod_files_pluvio[0])):
        element_remove =mod_files_pluvio[0]
        old_file = os.path.join(mypath_pluvio,file)
        new_file = os.path.join(mypath_pluvio, (file+".uploaded"))
        shutil.copy2(old_file, join(new_path_pluvio,file))
        os.rename(old_file,new_file)
        mod_files_pluvio.remove(element_remove)
        onlyfiles_pluvio.remove(file)
        print("lunghezza lista pluvio:", len(onlyfiles_pluvio))
        break

for file in onlyfiles_termo:
    if(file.find(mod_files_termo[0])):
        element_remove =mod_files_termo[0]
        old_file = os.path.join(mypath_termo,file)
        new_file = os.path.join(mypath_termo, (file+".uploaded"))
        shutil.copy2(old_file, join(new_path_termo,file))
        os.rename(old_file,new_file )
        mod_files_termo.remove(element_remove)
        onlyfiles_termo.remove(file)
        print("lunghezza lista termo:", len(onlyfiles_termo))
        break
