# METCS777-Term Project: Wedding Song Recommendation System
This repository contains the full implementation and experiments for our METCS777 Big Data Analytics term project paper. We evaluate how recommendation systems work with unsupervised bilingual data. The study compares K-means and GMM models with different features to identify the best clustering strategy.

## Project Overview:

**Project Title**: Wedding Song Recommendation Systems  

**By**: Steveen Vargas and Benjamin Lambright

**Project Purpose**: Provide a curated service for interacial/intercultural when selecting music for their wedding. 

**Motivation**: From the DJs contacted, most of them don't provide a service that would be able to dynamically suggest songs for the wedding. Therefore, we created a model where the bridge and the groom would be able to select their songs from data that has been curated and clustered for their wedding needs. 

**Method**: Data Preprocessing, Model Creation, song inference and final result.

**Outcome:** Find the best playlist for bilingual weddings.

## Dataset Information: Spotify Charts (All Audio Data)
- This is a complete dataset of all the "Top 200" and "Viral 50" charts published globally by Spotify. Spotify publishes a new chart every 2-3 days. This is its entire collection since January 1, 2019. This dataset is a continuation of the Kaggle Dataset: Spotify Charts but contains 29 columns for each row that was populated using the Spotify API.

## Dataset Sample
**numpy_array_for_modeling.csv** - This is the sample dataset that was initially used in our local system to test the model.
**numpy_array_for_modeling_with_cathegorical_columns.csv** - This is the sample dataset that was to test innitial results.
**big_data_ready_for_modeling.csv** - This is the final dataset used for modeling our clusters.

## Files Description
This repository contains code separate code versions that run on AWS and Google Cloud. 

**Dataframes:** 
- df_aws.py: Contains the code to run the sample data in AWS EMR
- df_gcp.py: Contains code to run the sample data in Google Cloud

**RDDS:**
- rdd_aws.py: Conttains code to run the sample data in AWS
- rdd_gcp_implement.py: Contains code to run sammple data in Google Cloud

---

## Dataset Explanation

Details of the dataset are available in `Dataset_attributes.pdf`.
The dataset originates from the Home Mortgage Disclosure Act (HMDA) database and includes:

Loan amount, interest rate, applicant income

Lender ID, loan purpose, and loan type

Applicant demographics and other relevant financial metrics

Only numeric features were used for model training to ensure computational efficiency.
Missing values were imputed and the dataset was standardized before training.

After preprocessing (cleaning, imputation, and feature selection), we used only **20%** of the total dataset for our analysis.

###  Reason for Sampling

Processing the entire HMDA dataset would have required **significantly higher computational resources, time, and storage** on both AWS and GCP.  
Since our **primary goal** was to **evaluate and compare the performance of different cloud platforms** (AWS vs GCP) and data abstractions (RDD vs DataFrame) **using the same dataset**, it was **not essential to use the entire dataset**.

To maintain a **balance between performance accuracy and cost efficiency**, we used a **20% representative sample** of the cleaned and preprocessed dataset.  
This sampling approach allowed us to:

-  **Reduce cluster runtime and overall computation cost**  
-  **Lower memory and processing load on Spark workers**  
-  **Preserve a statistically valid distribution** of key features and target variables for analysis  

This ensured that our performance evaluation remained **accurate, scalable, and cost-efficient**, without compromising the reliability of results.

---

## Environment Setup

Follow the steps below to configure and run the project on cloud platforms.

### AWS EMR Setup

1. **Add Bootstrap Script**
   - Include the `install_lib.sh` file under the **Bootstrap Actions** section during EMR cluster creation.  
   - This script installs all required Python libraries (NumPy, Pandas, PySpark, psutil, etc.).

2. **Set Up Logging**
   - Create a `logs/` folder in your **S3 bucket**.  
   - Add this path under **Cluster Logs** to capture runtime and step logs for monitoring.

3. **Upload and Run Scripts**
   - Upload the following files to your S3 bucket:  
     - `df_aws.py`  
     - `rdd_aws.py`  
   - Add them as **Steps** during cluster creation or submit them after cluster launch via the EMR console.

4. **Accessing Outputs**
   - After processing completes, output files will be generated in your S3 bucket:  
     - `hmda-rdd-aws-output.txt`  
     - `DF_aws.txt`  
   - These contain model metrics, system performance, and resource usage reports.

---

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

