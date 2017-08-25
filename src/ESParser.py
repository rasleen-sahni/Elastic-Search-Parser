import csv
import json
import sys
import os
import shutil

list_of_csvfiles = []


######################  ELASTICSEARCH FORMAT  ###########################

def ESparser(path,file,lab_name,scenario,run_date,test_name,guid_id):

    # Declaring variables for each file
    list_of_csvrows = []
    list_of_lru = []
    time_dict = {}
    timestamps = []
    csv_path = path + "/" + file
    lrumap_path = path + "/lrumap.txt"
    print("******************* " + file + " *******************")


    # Reading the LRU csv file
    with open(csv_path) as csvfile:
        reader = csv.reader(csvfile, delimiter=",")
        for row in reader:
            list_of_csvrows.append(row)
    host_name = list_of_csvrows[0][2]


    # Fetching lruType and lruSubType from lrumap.txt
    # TRY BLOCK
    try:
        with open(lrumap_path) as textfile:
            for line in textfile:
                list_of_lru.append(line)
        for lru in list_of_lru:
            if (lru.startswith(host_name)):
                lru_array = lru.split(':')
                lru_type = lru_array[1]
                lru_subtype = lru_array[2].split("\n")[0]


    # EXCEPT BLOCK - Executes when exception occurs
    except IOError:
        print("ERROR: LRU csv file cannot be found!")


    # ELSE BLOCK - Executes when no exception occurs
    else:
        # Creates a dictionary with key as timestamp and value as the rest elements in a csv line of the LRU csv file
        for list in list_of_csvrows:
            time = list[0]
            if (time in time_dict.keys()):
                time_dict[time].append(list[3:])
            else:
                time_dict[time] = [list[3:]]


        # Creates a list of distinct timestamps in the file
        for time in time_dict.keys():
            timestamps.append(time)
        sorted(timestamps)


        # Creating the  JSON structure similar to ElasticSearch
        ES_JSON_format = {
            "hits": []
        }


        # For each timestamp it creates the hits array
        for datetime in time_dict.keys():
            source = {
                "lab_name": "",
                "guid_id": "",
                "scenarioName": "",
                "artsId": "0",
                "testName": "",
                "timestamp": "",
                "runDate": "",
                "lruType": 44,
                "lruSubType": 1,
                "hostname": "",
                "eventData": {
                    "eventName": "",
                    "timestamp": ""
                },
                "lru_data": {
                    "CpuArray": [],
                    "IoArray": [],
                    "Mem": [],
                    "NetArray": [],
                    "ProcArray": []
                }
            }


            # Set the values for timestamp and lruDate in source key
            source["timestamp"] = datetime
            source["runDate"] = run_date
            source["lab_name"] = lab_name
            source["testName"] = test_name
            source["guid_id"] = guid_id
            source["hostname"] = host_name
            source["lruType"] = lru_type
            source["lruSubType"] = lru_subtype
            source["scenarioName"] = scenario


            # Fetching metric list for a particular timestamp
            metric_list = time_dict[datetime]


            # Declaring the metrics array
            cpu_array = []
            io_array = []
            mem_array = []
            proc_array = []
            net_array = []

            for metric in metric_list:

                if (metric[0] == "CPU"):
                    x = {}
                    cpu = metric[3].split(':')
                    x['name'] = cpu[0]
                    x['idle'] = cpu[4]
                    x['sys'] = cpu[3]
                    x['user'] = cpu[1]
                    x['iowait'] = cpu[5]
                    x['hirq'] = cpu[7]
                    x['sirq'] = cpu[8]
                    cpu_array.append(x)


                elif (metric[0] == "IO"):
                    x = {}
                    io = metric[3].split(':')
                    x['name'] = io[0]
                    x['rd_sectors'] = io[1]
                    x['wr_sectors'] = io[2]
                    x['rd_ios'] = io[3]
                    x['wr_ios'] = io[5]
                    x['in_flight'] = io[9]
                    x['rd_ticks'] = io[7]
                    x['wr_ticks'] = io[8]
                    x['rq_ticks'] = io[10]
                    x['tot_ticks'] = io[11]
                    io_array.append(x)


                elif (metric[0] == "MEM"):
                    x = {}
                    mem = metric[3].split(':')
                    x['swap'] = mem[0]
                    x['free'] = mem[1]
                    x['buff'] = mem[2]
                    x['cache'] = mem[3]
                    x['swapin'] = mem[4]
                    x['swapout'] = mem[5]
                    x['pgin'] = mem[6]
                    x['pgout'] = mem[7]
                    mem_array.append(x)


                elif (metric[0] == "NET"):
                    x = {}
                    net = metric[3].split(':')
                    x['name'] = net[0]
                    x['RxPackets'] = net[1]
                    x['RxBytes'] = net[2]
                    x['TxPackets'] = net[3]
                    x['TxBytes'] = net[4]
                    net_array.append(x)


                elif (metric[0] == "PROC"):
                    x = {}
                    proc = metric[3].split(':')
                    x['name'] = metric[1]
                    x['cpu'] = proc[0]
                    x['rss'] = proc[1]
                    x['proc_swap'] = proc[2]
                    x['rx_packets'] = proc[3]
                    x['rx_bytes'] = proc[4]
                    x['tx_packets'] = proc[5]
                    x['tx_bytes'] = proc[6]
                    proc_array.append(x)


            # Populating metrics arrays of hits array corresponding to each timestamp
            source["lru_data"]["CpuArray"] = cpu_array
            source["lru_data"]["IoArray"] = io_array
            source["lru_data"]["Mem"] = mem_array
            source["lru_data"]["NetArray"] = net_array
            source["lru_data"]["ProcArray"] = proc_array
            ES_JSON_format.get("hits").append(source)


        # Create a folder for each LRU inside the Output directory
        folder_name = "Output/" + host_name
        if not os.path.exists(folder_name):
            os.makedirs(folder_name)
        else:
            shutil.rmtree(folder_name)
            os.makedirs(folder_name)


        # Creating separate csv files corresponding to each timestamp in the LRU folder
        no_of_timestamps = 1
        for i in ES_JSON_format["hits"]:

            file = open(folder_name +"/" + host_name + "-" + str(no_of_timestamps) + ".json", "w")
            json.dump(i,file,sort_keys=False,indent=2)
            file.close()
            no_of_timestamps = no_of_timestamps + 1;



#######################   MAIN FUNCTION   ########################


def main(argv):

    print("\n")
    path = argv[1]                       # Example Path :  "avantsdl2/cap388/20170726/manual/165"
    path_array = path.split("/")
    lab_name = path_array[0]
    scenario = path_array[1]
    run_date = path_array[2]
    test_name = path_array[3]
    guid_id = path_array[4]


    # Creating list of all csv files in the path
    for file in os.listdir(path):
        if file.endswith(".csv"):
            if(file != "events.csv"):
                list_of_csvfiles.append(file)


    # Getting ES format for every csv file in the path
    for file in list_of_csvfiles:
        # Calling the ES Parser function
        ESparser(path,file,lab_name,scenario,run_date,test_name,guid_id)


if __name__ == "__main__":
    main(sys.argv)
