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
        
        if  len(event) == 0:       # RESIF client returns empty catalog but it does not get registered as empty. If web service returns empty dataframe delete it.
            event = None
    except:
        event = None
    
    return event

def loop_through_clients(starttime, endtime, minmag, headers):
    events_list = []
    for i in range(len(headers)):
        header = headers[i]
        events_list.append(get_event(header, starttime, endtime, minmag))
    return events_list

def convert_to_dataframe(cat, index, endtime):
    
    events = cat.__getitem__(index)
    
    et = []
    times = []
    lats = []
    lons = []
    deps = []
    magnitudes = []
    magnitudestype = []
    
    for event in events:
        if len(event.origins) != 0 and len(event.magnitudes) != 0:
            
            et.append(endtime)
            times.append(UTCDateTime(pd.Timestamp((event.origins[0].time.datetime), tz="UTC").timestamp()))           #event.origins[0].time.datetime
            lats.append(event.origins[0].latitude)
            lons.append(event.origins[0].longitude)
            if event.origins[0].depth is not None:
                deps.append(round(int(event.origins[0].depth)/1000, 1))                             #deps.append(round(int(event.origins[0].depth)/1000, 1))
            else:
                deps.append(0.0)
            magnitudes.append(round(event.magnitudes[0].mag, 1))
            magnitudestype.append(event.magnitudes[0].magnitude_type)
            
    df = pd.DataFrame({"endtime":et, "time":times, "lat":lats,"lon":lons,
                        "depth":deps, "mag":magnitudes,"type":magnitudestype})
    
    return df     

def f_time(event, savedevent):
    event_t, savedevent_t = event["time"], savedevent["time"]
    event_t = pd.Timestamp(f"{event_t}", tz="UTC").timestamp() 
    savedevent_t = pd.Timestamp(f"{savedevent_t}", tz="UTC").timestamp() 
    if (savedevent_t-30) <= event_t <= (savedevent_t+30):   # 
        return False
    else:
        return True
    
def f_coord(event, savedevent):
    event_lat, savedevent_lat = float(event["lat"]), float(savedevent["lat"])
    event_lon, savedevent_lon = float(event["lon"]), float(savedevent["lon"])

    if (savedevent_lat-0.5) <= event_lat <= (savedevent_lat+0.5) and (savedevent_lon-0.5) < event_lon < (savedevent_lon+0.5):
        return False
    else:
        return True

       
def f_mag(event, savedevent):
    event_mag, savedevent_mag = float(event["mag"]), float(savedevent["mag"])

    if (savedevent_mag-0.6) <= event_mag <= (savedevent_mag+0.6):
        return False
    else:
        return True

"""
def f_mag(event, savedevent):
    event_mag, savedevent_mag = float(event["mag"]), float(savedevent["mag"])
    event_type, savedevent_type = str(event["type"]), str(savedevent["type"])
    
    if event_type == savedevent_type:
        if (savedevent_mag-0.2) <= event_mag <= (savedevent_mag+0.2):
            return False
        else:
            return True
    elif event_type != savedevent_type:
        if (savedevent_mag-0.5) <= event_mag <= (savedevent_mag+0.5):
            return False
        else:
            return True
""" 

def findcopy(event, savedevent):
    # Returns True if the event is duplicate, returns False if event is unique 
    result = [f_time(event, savedevent), f_coord(event, savedevent), f_mag(event, savedevent)]
    return any(result)

def read_file(index, header):
    # Read the files
    dic = "catalog/"
    df_rd = pd.read_csv(f"{dic}{header[index]}.txt", sep=" ", names=["endtime", "time", "lat", "lon", "depth", "mag", "type"])
    #rd_str = df_rd.to_string(header=False, index =False)
    return df_rd[["time", "lat", "lon", "mag", "type"]]      


def create_files_with_headers(header, i):
    df = pd.DataFrame(columns=["endtime", "time", "lat", "lon", "depth", "mag", "type"])
    df.to_csv(f"catalog/{header[i]}.txt", index=None, sep=' ', mode='a')


def save_to_csv(cat, endtime, counter, header, lookBack):
    for i in range(len(cat)):      # i = web service index

        # Are there any event/events that have occured?
        if cat[i] == None:  # NO
            pass

        else:               # YES, new data available 
            event_df = convert_to_dataframe(cat, i, endtime)       # Convert obspy catalog object to pandas dataframe

            # Is this the first scan?
            if counter == 1:         # YES
                newcomers[i] = event_df[["time", "lat", "lon", "mag", "type"]]        # Save event date times into a list

            else:                    # NO 
                # Are there any date time data for this client in the duplicate list?
                trigger1 = 0 
                if len(newcomers[i]) == 0:   # NO
                    pass
                else:                        # YES
                    trigger1 = 1
                #print(trigger1)


                trigger2 = 0
                files = os.listdir("catalog/")
                for filename in files:
                    if filename[:-4] == header[i]: # YES
                        trigger2 = 1
                    else:
                        pass                      # NO
                #print(trigger2)

                # If trigger set to 0 file is going to be created for the first time
                if trigger1 == 0 and trigger2 == 0:
                    event_df.to_csv(f"catalog/{header[i]}.txt", header=None, index=None, sep=' ', mode='a')
                

                # Trigger is set to 1, check for duplicate events the write to txt file
                else:

                    #dups = 0       # List that contains previously captured event times                    
                    if trigger1 == 1 and counter < int(lookBack+1):
                        dups = newcomers[i]
                    else:
                        pass     
                    

                    if trigger2 == 1:
                        t2 = read_file(i, header)
                        if len(newcomers[i]) == 0:
                            dups = t2[["time", "lat", "lon", "mag", "type"]] 
                        else:
                            if "dups" in locals():
                                d_frames = [dups, t2]
                                dups = pd.concat(d_frames)
                            else:
                                dups = t2
                    else:
                        pass
                    
                        
                    df_list = event_df[["time", "lat", "lon", "mag", "type"]] 
                    
                    for t in range(len(df_list)):
                        trigger3 = 0
                        
                        for tp in range(len(dups)):
                            if findcopy(df_list.iloc[t], dups.iloc[tp]) == False:
                                trigger3 = 1     
                            else:
                                pass
                        
                        if trigger3 == 0:
                            #data = event_df.iloc[::-1].reset_index(drop=True).head()
                            #inv_ind = len(event_df) - (t+1)
                            event_copy = event_df.loc[t:t]             
                            event_copy.to_csv(f"catalog/{header[i]}.txt", index=None, header=None, sep=' ', mode='a')
                            event_copy = None
                        elif trigger3 == 1:
                            pass


   
# Registers
interval = 30            #Scanning interval
lookBack = 720         #Scan "lookBack" minutes past from current time
minmag = 3               #Minimum earthquake magnitude

timer = interval
counter = 0
newcomers = [ [],[],[],[],[], [],[],[],[],[], [],[],[],[],[], [],[],[],[],[], [],[],[],[],[] ]

headers = get_client_header()
#'BGR','EMSC','ETH','GEONET','GFZ','ICGC','INGV','IPGP','IRIS','ISC','KNMI'
# 0     1      2     3        4     5      6      7      8      9     10
#'KOERI','LMU','NCEDC','NIEP','NOA','ODC','ORFEUS','RASPISHAKE','RESIF'
# 11      12    13      14     15    16    17       18           19
#'SCEDC','TEXNET','UIB-NORSAR','USGS','USP'
# 20      21       22           23     24
headers_rev = []
select_header = [1, 3, 4, 6, 8, 9, 13, 15, 19, 20, 23, 24]
for h in select_header:
    headers_rev.append(headers[h])

while True:
    start = time.perf_counter()
    
    if timer >= interval:
        counter+=1
        endtime =  get_datetime()
        starttime = endtime - int(lookBack*60)
        print(f"{counter}  Checking clients...")
        cat = loop_through_clients(starttime, endtime, minmag, headers_rev)
        save_to_csv(cat, endtime, counter, headers_rev, lookBack)

        looptime = time.perf_counter() - start
        print(f"Done  Loop took {round(looptime,2)} seconds  Time:{endtime}")            # Loop took {round(looptime,2)} seconds  Time:{endtime.timetz()}
        timer = 0
        
    else:
        time.sleep(0.01)
        
    end = time.perf_counter()
    timer += (end-start)
