# METCS777-Term Project: Wedding Song Recommendation System
This repository contains the full implementation and experiments for our METCS777 Big Data Analytics term project paper. We evaluate how recommendation systems work with unsupervised bilingual data. The study compares K-means and GMM models with different features to identify the best clustering strategy.

## Project Overview:

**Project Title**: Wedding Song Recommendation Systems  

**By**: Steveen Vargas and Benjamin Lambright

**Project Purpose**: Provide a curated service for interacial/intercultural couples when selecting music for their wedding. 

**Motivation**: From the DJs contacted, most of them don't provide a service that would be able to dynamically suggest songs for the wedding. Therefore, we created a dynamic system for the groom and the bride to receive songs suggestions from the data that has been curated and clustered for their wedding needs. 

**Method**: Data Preprocessing, Model Creation, song inference and final result.

**Outcome:** Find the best playlist for bilingual weddings.

## Dataset Information: Spotify Charts (All Audio Data)
- This is a complete dataset of all the "Top 200" and "Viral 50" charts published globally by Spotify. Spotify publishes a new chart every 2-3 days. This is its entire collection since January 1, 2019. This dataset is a continuation of the Kaggle Dataset: Spotify Charts but contains 29 columns for each row that was populated using the Spotify API.

## Dataset Sample
**numpy_array_for_modeling.csv** - This is the sample dataset that was initially used in our local system to test the model.

**numpy_array_for_modeling_with_cathegorical_columns.csv** - This is the sample dataset that was to test innitial results.

**big_data_ready_for_modeling.csv** - This is the final dataset used for modeling our clusters.

## Files Description
This repository contains code separate code versions that run on Google Cloud. 

**Pre-Processing:** 
- **preprocessing.ipynb:** Contains the code to pre-process the language detection, remove unwanted columns, drop duplicate rows, implementing correct data types, remove explicit songs, and add language_id column.
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
27.	Column Name: af_valence 
  a.	Data Type: INT
  b.	Range: 0 to 1
  c.	Description: Describes the musical positiveness of the track, where high values represent positive valence (e.g., happy, cheerful) and low values represent negative valence (e.g., sad, depressed).
28.	Column Name: af_tempo
  a.	Data Type: INT
  b.	Range: 0 to 238
  c.	Description: The tempo of the track in beats per minute (BPM).
29.	Column Name: af_time_signature
  a.	Data Type: Categorical
  b.	Range: 0 to 5
  c.	Description: The time signature of the track, indicating the number of beats in each bar and the type of note that receives one beat.

To build the K-means and GMM models only numeric features were used in training to ensure computational efficiency.
Missing rows were deleted or imputed and the dataset was standardized before training.

After preprocessing , we used all of the resulting data our analysis. We noticed that most charting songs tend to repeat in many countries so we had to take care of duplicates.

---

## Environment Setup

Follow the steps below to configure and run the project on cloud platforms.

### Google Cloud Dataproc Setup

1. **Upload Files**
   - Upload all `.py` scripts and datasets to your **Google Cloud Storage** bucket:  
     - `rdd_gcp_implement.py`  
     - `df_gcp.py`

2. **Create Cluster**
   - Create a **Dataproc cluster** with PySpark pre-installed, or install dependencies manually.

3. **Submit Job**
   - Provide the dataset path and desired output location as arguments during job submission.  
   - Once completed, retrieve the output files from the bucket.

4. **Retrieve Outputs**
   - Output files generated:  
     - `hmda_rdd_gcp_output.txt`  
     - `df_gcp_output.txt`  
   - These files contain the same performance and accuracy metrics as the AWS results.

---

## How to Run the Code
You can run the code in **two ways**:
1. **Through the Cloud Console UI (Recommended)**  
   - Both AWS EMR and GCP Dataproc allow submitting jobs directly through their web interfaces without using the command line.  
   - Simply choose “Add Step” (AWS) or “Submit Job” (GCP), upload your script, and specify input/output paths.

2. **Using Command Line (Optional)**
   
```bash
# Execute on AWS EMR
python3 df_aws.py
python3 rdd_aws.py

### On Google Cloud:
# Execute on Google Cloud Dataproc
python3 df_gcp.py
python3 rdd_gcp_implement.py
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
- Script: `gs://metcs777termpaper/df_gcp.py`  
- Input Dataset: `gs://metcs777termpaper/hmda_2016_nationwide_all-records_labels.csv`  
- Output Directory: `gs://metcs777termpaper/hmda_test/`


---
## Results and Observations

The generated output files (`hmda-rdd-aws-output.txt`, `DF_aws.txt`, `hmda_rdd_gcp_output.txt`, `DF_gcp.txt`) contain:

## Performance Metrics

Add pending information

## Model Evaluation

Add information here:

