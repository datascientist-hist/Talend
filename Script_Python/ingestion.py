## ---------------------
## Caricamento librerie
## ---------------------

import pandas as pd
import os
from os import listdir
from os.path import isfile, join
import time
from datetime import datetime, date
import shutil
import sys

## ---------------------------------
## Settando i path acquisiti da bash
## ---------------------------------

path_input = sys.argv[1]
path_output =sys.argv[2]
mypath_pluvio = path_input+"/Pluvio/"
mypath_termo  = path_input+"/Termo/"
#mypath_pluvio = "C:/Users/pepee/Desktop/Tirocinio/Talend/DatiETL/Pluvio/"
#mypath_termo= "C:/Users/pepee/Desktop/Tirocinio/Talend/DatiETL/Termo/"


## ------------------------------------
## Creazione liste file Pluvio e Termo
## ------------------------------------

onlyfiles_pluvio = [f for f in listdir(mypath_pluvio) if isfile(join(mypath_pluvio, f)) and f.find(".uploaded") == -1]
onlyfiles_termo= [f for f in listdir(mypath_termo) if isfile(join(mypath_termo, f)) and f.find(".uploaded") == -1]

## --------------------------------------------------------------------------
## Cerco la data nel nome del file per porterli ordinare in ordine cronologico
## --------------------------------------------------------------------------
header_pluvio="DatiPluvio_" #start at 11-21
mod_files_pluvio=[]
for file in onlyfiles_pluvio:
    mod_files_pluvio.append(file[11:21])

mod_files_pluvio
mod_files_pluvio.sort(key=lambda x: datetime.strptime(x, '%Y-%m-%d'),reverse=False)

## --------------------------------------------------------------------------
## Cerco la data nel nome del file per porterli ordinare in ordine cronologico
## --------------------------------------------------------------------------
header_termo="DatiTermo_" #start at 10-20
mod_files_termo=[]
for file in onlyfiles_termo:
    mod_files_termo.append(file[10:20])

mod_files_termo
mod_files_termo.sort(key=lambda x: datetime.strptime(x, '%Y-%m-%d'),reverse=False)


#new_path_pluvio="C:/Users/pepee/Desktop/Tirocinio/Talend/Dati_Giornalieri/"
#new_path_termo="C:/Users/pepee/Desktop/Tirocinio/Talend/Dati_Giornalieri/"

## ------------------------------------------------------------------------------
## Sposto un solo file Pluvio in ordine cronologico da DatiETl a Dati Giornalieri
## ------------------------------------------------------------------------------
for file in onlyfiles_pluvio:
    if(file.find(mod_files_pluvio[0])):
        element_remove =mod_files_pluvio[0]
        old_file = os.path.join(mypath_pluvio,file)
        new_file = os.path.join(mypath_pluvio, (file+".uploaded"))
        shutil.copy2(old_file, join(path_output,file))
        os.rename(old_file,new_file)
        mod_files_pluvio.remove(element_remove)
        onlyfiles_pluvio.remove(file)
        print("lunghezza lista pluvio:", len(onlyfiles_pluvio))
        break
## ------------------------------------------------------------------------------
## Sposto un solo file Termo in ordine cronologico da DatiETl a Dati Giornalieri
## ------------------------------------------------------------------------------
for file in onlyfiles_termo:
    if(file.find(mod_files_termo[0])):
        element_remove =mod_files_termo[0]
        old_file = os.path.join(mypath_termo,file)
        new_file = os.path.join(mypath_termo, (file+".uploaded"))
        shutil.copy2(old_file, join(path_output,file))
        os.rename(old_file,new_file )
        mod_files_termo.remove(element_remove)
        onlyfiles_termo.remove(file)
        print("lunghezza lista termo:", len(onlyfiles_termo))
        break
