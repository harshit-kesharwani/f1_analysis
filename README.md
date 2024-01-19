# Formula 1 Race Data Engineering 

This project is an end-to-end Databricks project that utilizes the latest PySpark and SQL capabilities.

## Tech Stack
- **Platform:** Databricks [Premium Tier]
- **Cloud Platform Tools:** Azure Key Vault, Data Lake Gen 2 Storage, IAM, Service Account
- **Language:** Python, SQL

## Features
- Ingest data of different data formats
- Full load and incremental load capabilities
- Delta live tables allowing time travel
- POC Folder containing additional information and learning aspects.

## Documentation
* Please note the following setup needs to be done before running the project. There may be some configuration issues while setting up the project, so please ensure proper configuration rights.
    - For setup, ingest the raw files in the raw folder of your Azure storage account.
    - Mount storage.
    - Set up Key Vault.
- Storage mounting script along with different storage access options can be found in the setup folder.
- The project starts with ingesting files, which can be done by running all notebooks in the ingestion folder.
- For better visualization, we may create tables of raw data as well with the help of create_raw_tables in the raw folder.
- After ingestion, we can perform transformation by running transformation notebooks in sequence.
- All the common functions can be checked in the includes folder along with the configuration locations of raw, processed, and presentation layer variables.
- Please note the raw data contains only three date files: cut-over file of 21st March 2021, incremental file of 28 March 2021, and 18th April 2021.
- **The prod equivalent code can be checked in the prod folder, but for a better understanding of the process, I recommend going through the normal notebook files as they contain thoroughly explained code.**

## Lessons Learned
- Databricks Functionalities
- Delta live tables
- Data Analytics
- Ingestion, Transformation, and Presentation
- Azure tools use in Data Engineering
- Azure Data Factory

## Feedback
If you have any feedback, please reach out to me at harshitkesharwani18@gmail.com

## 🔗 Social

[![linkedin](https://img.shields.io/badge/linkedin-0A66C2?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/harshitkesharwani/)
[![twitter](https://img.shields.io/badge/Instagram-E4405F?style=for-the-badge&logo=instagram&logoColor=white)](https://www.instagram.com/harshit.kesharvani/)


## Acknowledgements

 - [This project has been built with the learnings from Udemy Course By Ramesh Retnasamy](https://www.udemy.com/course/azure-databricks-spark-core-for-data-engineers/?utm_source=adwords-pmax&utm_medium=udemyads&utm_campaign=PMax_la.EN_cc.INDIA&utm_content=deal4584&utm_term=_._ag__._kw__._ad__._de_c_._dm__._pl__._ti__._li_9050497_._pd__._&gad_source=1&gclid=Cj0KCQiAtaOtBhCwARIsAN_x-3IIfVksKQDFMRfA2eA_TDxUyDb9ezJ_3XFxqiavbYzgTVXyF4X4s5kaAsoIEALw_wcB)





## License
[![MIT License](https://img.shields.io/badge/License-MIT-green.svg)](https://choosealicense.com/licenses/mit/)
