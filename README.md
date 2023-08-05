## SPARK PROJECT

**Submission By: Abhinandan Aryal & Kassem Bou Zeid**

The assignment was to perform several analysis on NYC Taxi dataset using spark. For this purpose, GCP was used and a cluster was made to run the analysis.

## CLONING THE PROJECT

The project can be cloned using the link provided in the clone option. After that following steps need to be taken to run the project for the linux environment as our environment was LINUX.

## INSTALLING GCLOUD CLI

1. Download the CLI
```bash
curl -O https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-441.0.0-linux-x86_64.tar.gz
```

2. Unzipping the File
```bash
tar -xf google-cloud-cli-441.0.0-linux-x86_64.tar.gz
```

3. Installing the CLI
```bash
./google-cloud-sdk/install.sh
```

4. Initialize the CLI
```bash
./google-cloud-sdk/bin/gcloud init
```

## CREATING THE CLUSTER

A cluster was created in GCP to run the analysis using the following command:
```bash
make create_cluster
```

## RUNNING THE JOBS

The jobs are organized inside the `src/jobs` folder. Each job has its own folder. For running each job, we changed the job name in the `.env` file and used make to submit the job to GCP using the following command.

```bash
make submit_job
```

## VIEWING THE RESULTS

After each job is run, the results are stored in the bucket `pyspark-tutorial-abhinandan-aryal`. The results are stored as per the task number. The number in front of the results refer to the task number in the following order.

1. Trip Analysis
2. Tip Analysis
3. Fare Analysis
4. Traffic Analysis
5. Prediction Job

Multiple files are stored as results for each job to ensure the intermediate results as per necessity.




