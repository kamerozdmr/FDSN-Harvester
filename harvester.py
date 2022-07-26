import sys
import os
import time
import numpy as np
import pandas as pd
import openpyxl
from obspy.clients.fdsn import Client
from obspy import UTCDateTime
from openpyxl import load_workbook
from openpyxl import Workbook

def get_datetime():
    return UTCDateTime.now()

def get_client_header():
    from obspy.clients.fdsn.header import URL_MAPPINGS
    return sorted(URL_MAPPINGS.keys())

def get_event(header, starttime, endtime, minmag):
    client = Client(header)
    try:
        event = client.get_events(minmagnitude=minmag, starttime=starttime, endtime=endtime)
        
        # RESIF client returns empty catalog but it does not get registered as empty. If it is empty delete it.
        if  len(event) == 0:
            event = None
    except:
        event = None
    
    return event

def loop_through_clients(starttime, endtime, minmag):
    events_list = []
    headers = get_client_header()
    for i in range(len(headers)):
        header = headers[i]
        events_list.append(get_event(header, starttime, endtime, minmag))
    return events_list
    
def convert_to_dataframe(cat, index, starttime, endtime):
    
    events = cat.__getitem__(index)
    
    st = []
    et = []
    times = []
    lats = []
    lons = []
    deps = []
    magnitudes = []
    magnitudestype = []
    
    for event in events:
        if len(event.origins) != 0 and len(event.magnitudes) != 0:
            st.append(starttime)
            et.append(endtime)
            times.append(event.origins[0].time.datetime)
            lats.append(event.origins[0].latitude)
            lons.append(event.origins[0].longitude)
            deps.append(event.origins[0].depth)
            magnitudes.append(event.magnitudes[0].mag)
            magnitudestype.append(event.magnitudes[0].magnitude_type )
            
    df = pd.DataFrame({"starttime":st, "endtime":et, "time":times, "lat":lats,"lon":lons,
                        "depth":deps, "mag":magnitudes,"type":magnitudestype})
    
    return df.iloc[::-1]

  
def read_file(index):
    # Read the files
    dic = "catalog/"
    df_rd = pd.read_csv(f"{dic}{get_client_header()[index]}.txt", sep=" ", names=["starttime", "endtime", "time", "lat","lon","depth", "mag","type"])
    #rd_str = df_rd.to_string(header=False, index =False)
    return df_rd["time"]


def create_files_with_headers(header, i):
    df = pd.DataFrame(columns=["starttime", "endtime", "time", "lat","lon","depth", "mag","type"])
    df.to_csv(f"catalog/{header[i]}.txt", index=None, sep=' ', mode='a')
       
       
def save_to_csv(cat, starttime, endtime, counter):
    header = get_client_header()

    for i in range(len(cat)):

        # Are there any event/events that have occured?
        if cat[i] == None:  # NO
            pass


        else:               # YES, new data available 
            event_df = convert_to_dataframe(cat, i, starttime, endtime)       # Convert obspy catalog object to pandas dataframe

            # Is this the first scan?
            if counter == 1:         # YES
                newcomers[i] = (convert_to_dataframe(cat, i, starttime, endtime)["time"])       # Save event date times into a list


            else:                    # NO
                         
                # Are there any date time data for this client in the duplicate list?
                
                trigger1 = 0 
                if len(newcomers[i]) == 0:   # NO
                    pass
                else:                        # YES
                    trigger1 = 1
                print(trigger1)


                trigger2 = 0
                files = os.listdir("catalog/")
                for filename in files:
                    if filename[:-4] == header[i]: # YES
                        trigger2 = 1
                    else:
                        pass                      # NO
                print(trigger2)

                # If trigger set to 0 file is going to be created for the first time
                if trigger1 == 0 and trigger2 == 0:
                    event_df.to_csv(f"catalog/{header[i]}.txt", header=None, index=None, sep=' ', mode='a')
                

                # Trigger is set to 1, check for duplicate events the write to txt file
                else:
                    dups = []       # List that contains previously captured event times                    
                    if trigger1 == 1 and counter < 31:
                        t1 = newcomers[i]
                        for t in t1:
                            dups.append(t)   
                    else:
                        pass     
                    

                    if trigger2 == 1:
                        t2 = read_file(i)
                        for t in range(len(t2)):
                            df_dt = t2.loc[t:t]
                            dt = df_dt.to_string()[5:]
                            dups.append(pd.Timestamp(f'{dt}', tz=None))
                    else:
                        pass
                        

                    #print(f"dups {dups}")

                    df_list = []
                    for t in range(len(event_df["time"])):
                        df_list.append(event_df["time"][t])
                    
                    #print(f"df list {df_list}")
                    for t in range(len(df_list)):
                        trigger3 = 0
                        for tp in range(len(dups)):
                            if (dups[tp] == df_list[t]): 
                                trigger3 = 1     
                            else:
                                pass
                        
                        if trigger3 == 0:
                            event_copy = event_df.loc[t:t]             
                            event_copy.to_csv(f"catalog/{header[i]}.txt", index=None, header=None, sep=' ', mode='a')
                            event_copy = None
                        else:
                            pass




    
# Registers
interval = 60            #Scanning interval
lookBack = 30*60         #Scan "lookBack" seconds past from current time
minmag = 1               #Minimum earthquake magnitude

timer = interval
counter = 0
#newcomers = np.empty(shape=(25,1),dtype='object')
newcomers = [ [],[],[],[],[], [],[],[],[],[], [],[],[],[],[], [],[],[],[],[], [],[],[],[],[] ]

while True:
    start = time.perf_counter()
    
    if timer >= interval:
        counter+=1
        endtime =  get_datetime()
        starttime = endtime - lookBack
        print(f"{counter}  Checking clients...")
        x = loop_through_clients(starttime, endtime, minmag)
        print(x)
        save_to_csv(x, starttime, endtime, counter)
        
        print(f"Done  {endtime}")
        timer = 0
        
    else:
        time.sleep(0.01)
        
    
    end = time.perf_counter()
    timer += (end-start)
    
    