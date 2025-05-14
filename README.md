# DE_Final_Project

# To test ingestion separately:

Go to the ingest folder and then the modular ingestion folder. It contains the script used to ingest from each source stated



To test combined ingestion flow i.e. data ingested from all sources, processed and uploaded to DB:
 
Go to the ingest folder and then the combined ingestion folder. 



# To view or test individual ML scripts:

Go to Modular ML files folder


# To run the Prefect flow:

Assuming you are in an environment that has all the required installations. Start the Prefect worker by running:

 	prefect worker start --pool default

This worker is responsible for picking up scheduled and manual flow runs. To manually trigger the data pipeline (e.g. for testing or demonstration purposes), use the command:

 	prefect deployment run 'daily_orchestrator_flow/car-data-pipeline' 

This will launch the entire data pipeline, which includes ingestion from external sources (PakWheels, PSO, PAMA, PBS), preprocessing, and machine learning model training. As long as the Prefect worker is running in the terminal, scheduled flows will continue to execute automatically based on their configured timing.

All the files needed for this are in the Prefect-flow folder. A screenshot of the working pipeline has been attached in the report



# To view Streamlit UI (shown in demo video):

Note that since my DB is local the backend won't run on a different device. All the files required to run are in the Streamlit-localhost-setup folder.

If not already trained, run: python model_training.py

This will train and save models including:

revenue_optimizer_model.pkl (used by the Revenue Optimizer tab)

Then run: streamlit run app.py

