# Airflow ETL Pipeline – Consumer Complaints

This project is a **Data Engineering Project** that shows how to build an **ETL pipeline with Airflow**.  
The pipeline fetches consumer complaints data from the **Consumer Financial Protection Bureau (CFPB) API**, processes it, and then loads it into a **MySQL database** and **Google Sheets**.

---

## 📂 Project Files
- `etl/extract.py` → Extracts data from CFPB API for all US states  
- `etl/transform.py` → Cleans and transforms the data  
- `etl/load.py` → Loads data into MySQL and Google Sheets  
- `etl_dag.py` → Airflow DAG for orchestration  
- `consumer_complaints_transformed.csv` → Final transformed dataset  

---

## 🔄 Workflow
1. **Extract** → API data is pulled for all states  
2. **Transform** → Data cleaned and saved as CSV  
3. **Load** → Data pushed to MySQL + Google Sheets  
4. **Orchestrate** → Tasks managed by Airflow DAG  
5. **Notify** → Email sent after successful run  

---

## 🛠️ Tools Used
- Python, Airflow, Pandas, Requests  
- MySQL  
- Google Sheets API  
- Email Notifications (Airflow EmailOperator)  

---

## 📸 Outputs
- Clean CSV file (`consumer_complaints_transformed.csv`)  
- Data in MySQL `Consumer_Complaints` table  
- Data available in Google Sheets  
- Airflow DAG Graph + Grid view screenshots  
- Email notification proof

---

### 👩‍💻 Author
**Jeiya Kumari**  
(Data Engineering project)
