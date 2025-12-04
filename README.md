# METCS777-Term Project: Wedding Song Recommendation System
This repository contains the full implementation and experiments for our METCS777 Big Data Analytics term project paper. We evaluate how recommendation systems work with unsupervised bilingual data. The study compares K-means and GMM models with different features to identify the best clustering strategy.

## Project Overview:

**Project Title**: Wedding Song Recommendation Systems  

**By**: Steveen Vargas and Benjamin Lambright

**Project Purpose**: Provide a curated service for interracial/intercultural couples when selecting music for their wedding. 

**Motivation**: In the wedding industry, when you are trying to hire an entertainment company. Some of the DJ’s approach is to send a google form or an excel sheet to your guests where they would obtain the songs that the family would like. They don't provide a service that would be able to dynamically suggest songs for the wedding automatically as the users are entering the songs that they would like to dance to. Therefore, we created a dynamic system for the groom and the bride to obtain a curated list of songs that matches their guest requests and provides similar songs.
**Method**: Data Preprocessing, Model Creation, song inference and final result.

**Outcome:** Find the best playlist for bilingual weddings.

## Dataset Information: Spotify Charts (All Audio Data)
- This is a complete dataset of all the "Top 200" and "Viral 50" charts published globally by Spotify. Spotify publishes a new chart every 2-3 days. This is its entire collection since January 1, 2019. This dataset is a continuation of the Kaggle Dataset: Spotify Charts but contains 29 columns for each row that was populated using the Spotify API.

## Dataset Sample
**numpy_array_for_modeling.csv** - This is the sample dataset that was initially used in our local system to test the model.

**numpy_array_for_modeling_with_cathegorical_columns.csv** - This is the sample dataset that was to test initial results.

**big_data_ready_for_modeling.csv** - This is the final dataset used for modeling our clusters.

## Files Description
This repository contains separate code versions that run on Google Cloud. 

**Pre-Processing:** 
- **preprocessing.ipynb:** Contains the code to pre-process the language detection, remove unwanted columns, drop duplicate rows, implement correct data types, remove explicit songs, and add language_id column.
- **large_scale_langid.ipynb**: Contains code to detect most languages in the dataset

**Modeling:**
- **METCS777-term-project-code-Team9-GMM-Implementation.ipynb:** Contains code to run GMM model and cluster songs depending on its features. 
- **METCS777-term-project-code-Team9-Kmeans-Implementation.ipynb:** Contains code to run K-means model and cluster songs depending on its features.
  
**Inference:**
- add_file: Conttains code to run.... 
- add_file: Contains code to run... 
---

## Dataset Exploration:

Details of the dataset are available on `Spotify Charts (All Audio Data) Data Set Dictionary.pdf`. It contains column name, datatype, range information and description.

Example: 

Column Name: **af_valence**
- Data Type: INT
- Range: 0 to 1
- Description: Describes the musical positiveness of the track, where high values represent positive valence (e.g., happy, cheerful) and low values represent negative valence (e.g., sad, depressed).


To build the K-means and GMM models only numeric features were used in training to ensure computational efficiency.

Missing rows were deleted or imputed, and the dataset was standardized before training.

After preprocessing , we used all of the resulting data for our analysis. We noticed that most charting songs tend to repeat in many countries, so we had to take care of duplicates.

---

## Environment Setup

Follow the steps below to configure and run the project on cloud platforms.

### Google Cloud Dataproc Setup

1. **Upload Files**
   - Upload all `.py` scripts and datasets to your **Google Cloud Storage** bucket:  
     - `METCS777-term-project-code-Team9-GMM-Implementation.py`  
     - `METCS777-term-project-code-Team9-Kmeans-Implementation.py`

2. **Create Cluster**
   - Create a **Dataproc cluster** with PySpark pre-installed, or install dependencies manually.

3. **Submit Job**
   - Provide the dataset path and desired output location as arguments during job submission.  
   - Once completed, retrieve the output files from the bucket.

4. **Retrieve Outputs**
   - Output files generated:  
     - `Folder: big_data_clusters`   
   - This folder contain the CSV files with cluster song information to perform inference.

---

## How to Run the Code
You can run the code in **two ways**:
1. **Through the Cloud Console UI (Recommended)**  
   - GCP Dataproc allows submitting jobs directly through their web interfaces without using the command line.  
   - Simply choose “Submit Job” (GCP), upload your script, and specify input/output paths.

2. **Using Command Line (Optional)**
   
```bash
### On Google Cloud:
# Execute on Google Cloud Dataproc
python3 example_file.py
python3 example_file.py
```
Each script automatically logs execution details, saves the results to the specified bucket, and prints key performance metrics.

---
### Execution Paths for Cloud Platforms

Below are the general and actual paths used for running the PySpark scripts on both AWS and Google Cloud.

---

#### **Generalized Paths**


**For Google Cloud (Modeling):**  
- `gs://<bucket_name>/<script_filename>.py`  
- `gs://<bucket_name>/<input_dataset>.csv`  
- `gs://<bucket_name>/<output_directory>/`

>  Ensure your bucket names, file paths, and IAM permissions are correctly configured before execution.

---

#### **Actual Paths Used in This Project**



**For Google Cloud:**  
- Script: `gs://metcs777termprojectpaper/df_gcp.py`  
- Input Dataset: `gs://metcs777termpaper/hmda_2016_nationwide_all-records_labels.csv`  
- Output Directory: `gs://metcs777termpaper/hmda_test/`


---
## Results and Observations

The generated output files (`big_data_clusters`) contain:

- cluster_0_big_data.csv
- cluster_1_big_data.csv
- cluster_2_big_data.csv
- cluster_3_big_data.csv
- cluster_4_big_data.csv
- cluster_5_big_data.csv
- cluster_6_big_data.csv
- cluster_7_big_data.csv
- cluster_8_big_data.csv
- cluster_9_big_data.csv
- cluster_10_big_data.csv
- cluster_11_big_data.csv
- cluster_12_big_data.csv
- cluster_13_big_data.csv


## Evaluation
- Our data was difficult to cluster and did not provide great silhouette results when working with k-means or GMM. There was a bit more distribution when clusters were created in GMM, but it was still difficult to have a balanced clustering result.
-  Spotify measures up-to-date trending, information we don't have but we could have obtained it with more time.
-  In the future we would like to connect our project to the Spotify API.
-  Similarly, something that would have made a difference would be to improve the language detection a featurization. Something that it's difficult to do with free tools. 

