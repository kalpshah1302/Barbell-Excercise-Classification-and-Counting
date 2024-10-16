import pandas as pd
from glob import glob


# Read single files


single_file_acc= pd.read_csv("../../data/raw/MetaMotion/A-bench-heavy2-rpe8_MetaWear_2019-01-11T16.10.08.270_C42732BE255C_Accelerometer_12.500Hz_1.4.4.csv")


single_file_gyr= pd.read_csv("../../data/raw/MetaMotion/A-bench-heavy2-rpe8_MetaWear_2019-01-11T16.10.08.270_C42732BE255C_Gyroscope_25.000Hz_1.4.4.csv")








# List all data files so that i can loop over finally to the whole data

files = glob("../../data/raw/MetaMotion/*.csv")

len(files)

files[0]


# --------------------------------------------------------------
# Extract features from filename
# --------------------------------------------------------------

data_path= "../../data/raw/MetaMotion\\"
f= files[0]

participant =f.split("-")[0].replace(data_path,"")  # replaced the whole data_path string with empty string


label =f.split("-")[1]

category=f.split("-")[2].rstrip("123").rstrip("_MetaWear_2019") # rstrip removes any character that we specify , as here there were either 1, 2 or 3 sets so we wrote 123


df= pd.read_csv(f)


df["participant"]= participant
df["label"]= label
df["category"]= category

# --------------------------------------------------------------
# Read all files
# --------------------------------------------------------------

acc_df=pd.DataFrame()
gyr_df= pd.DataFrame()

acc_set=1
gyr_set=1


for f in files:
    
    participant =f.split("-")[0].replace(data_path,"") 

    label =f.split("-")[1]

    category=f.split("-")[2].rstrip("123").rstrip("_MetaWear_2019")
    # print(category) 
    
    df= pd.read_csv(f)


    df["participant"]= participant
    df["label"]= label
    df["category"]= category
    
    if "Accelerometer" in f:
        df["set"]=acc_set
        acc_set += 1
        
        acc_df=pd.concat([acc_df,df])
        

    if "Gyroscope" in f:
        df["set"]=gyr_set
        gyr_set += 1
        
        gyr_df=pd.concat([gyr_df,df])
        
        
# acc_df[acc_df["set"]==10] 

       
        
# --------------------------------------------------------------
# Working with datetimes
# --------------------------------------------------------------


acc_df.info()
pd.to_datetime(df["epoch (ms)"], unit="ms")


# pd.to_datetime(df["time (01:00)"]).dt.month

acc_df.index = pd.to_datetime(acc_df["epoch (ms)"], unit= "ms")
gyr_df.index = pd.to_datetime(gyr_df["epoch (ms)"], unit= "ms")

del acc_df["epoch (ms)"]
del acc_df["time (01:00)"]
del acc_df["elapsed (s)"]



del gyr_df["epoch (ms)"]
del gyr_df["time (01:00)"]
del gyr_df["elapsed (s)"]
# --------------------------------------------------------------
# Turn into function ONLY IMPORTANT THING , COMMENT REST OF IT
# --------------------------------------------------------------

files = glob("../../data/raw/MetaMotion/*.csv")


def read_data_from_files(files):
    acc_df=pd.DataFrame()
    gyr_df= pd.DataFrame()

    acc_set=1
    gyr_set=1


    for f in files:
        
        participant =f.split("-")[0].replace(data_path,"") 

        label =f.split("-")[1]

        category=f.split("-")[2].rstrip("123").rstrip("_MetaWear_2019")
        # print(category) 
        
        df= pd.read_csv(f)


        df["participant"]= participant
        df["label"]= label
        df["category"]= category
        
        if "Accelerometer" in f:
            df["set"]=acc_set
            acc_set += 1
            
            acc_df=pd.concat([acc_df,df])
            

        if "Gyroscope" in f:
            df["set"]=gyr_set
            gyr_set += 1
            
            gyr_df=pd.concat([gyr_df,df])
    
    acc_df.index = pd.to_datetime(acc_df["epoch (ms)"], unit= "ms")
    gyr_df.index = pd.to_datetime(gyr_df["epoch (ms)"], unit= "ms")

    del acc_df["epoch (ms)"]
    del acc_df["time (01:00)"]
    del acc_df["elapsed (s)"]



    del gyr_df["epoch (ms)"]
    del gyr_df["time (01:00)"]
    del gyr_df["elapsed (s)"]        
    
    return acc_df,gyr_df
            

acc_df,gyr_df= read_data_from_files(files)


# --------------------------------------------------------------
# Merging datasets
# --------------------------------------------------------------

data_merged= pd.concat([acc_df.iloc[:,:3],gyr_df], axis=1) # to select dirst 3 columns so as to remove redundant columns , [:,:] writing 3 afterwards selects first 3 columns.....first 3 columns from accelerometer data and rest from gyr


data_merged.columns= [
    
    "acc_x",
    "acc_y",
    "acc_z",
    "gyr_x",
    "gyr_y",
    "gyr_z",
    "participant",
    "label",
    "category",
    "set",
    ]



# --------------------------------------------------------------
# Resample data (frequency conversion)
# --------------------------------------------------------------

# Accelerometer:    12.500HZ ---every 0.08 sec
# Gyroscope:        25.000Hz  --- every 0.16 sec

sampling={
   
    "acc_x": "mean",
    "acc_y": "mean",
    "acc_z": "mean",
    "gyr_x": "mean",
    "gyr_y": "mean",
    "gyr_z": "mean",
    "participant": "last",
    "label": "last",
    "category": "last",
    "set": "last",
}



data_merged[:100].resample(rule="200ms").apply(sampling)

#split by day

days = [g for n , g in data_merged.groupby(pd.Grouper(freq="D"))]

data_resampled=pd.concat([df.resample(rule="200ms").apply(sampling).dropna() for df in days])


data_resampled["set"]= data_resampled["set"].astype("int")
data_resampled.info()
# --------------------------------------------------------------
# Export dataset
# why to pickle -- good for working with time stamps
data_resampled.to_pickle("../../data/interim/01_data_processed.pkl")

# --------------------------------------------------------------