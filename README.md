# Docker-based PySpark Distributed Computing Environment

---

This is a guide to set up an environment that provides a ready-to-use Apache Spark cluster containing **one Spark Master**, 
<br> **N Spark Workers**, and a **JupyterLab** environment for running PySpark notebooks.


## Prerequisites
Ensure you have the following installed on your local machine:
* **Docker** and **Docker Compose**
* **A Web browser**
* **A Python IDE with support for Jupyter notebooks**. Here we are using PyCharm.

> 💡 *Note: No local installation of Apache Spark or PySpark is required.*

---

## 🚀 Getting Started

### 1. Start the Spark Cluster
Open a terminal in the project directory and launch the cluster with your desired number of workers (e.g., 3 workers):
```bash
docker compose up -d --scale spark-worker=3
```

### 2. Verify the Cluster Status
Check if all containers are running properly:
```bash
docker compose ps
```
**Expected Output:**
* `spark-master`
* `spark-spark-worker-1`
* `spark-spark-worker-2`
* `spark-spark-worker-3`
* `jupyter`

### 3. Access the Spark UI
Open your browser and navigate to **http://localhost:8080** to view the Spark execution environment status and details.

---

## 💻 IDE Integration (PyCharm)

You can open the root project directory directly in PyCharm or your preferred IDE. 

### Project Structure
The following folders are mapped directly inside the Docker containers:
* `notebooks/` — Stores the Jupyter notebooks.
* `apps/` — Place your standalone Python applications here.
* `data/` — Contains datasets to be read by your code.

### 🔌 Connect the IDE to the Jupyter Server
The JupyterLab instance is available at **http://localhost:8888**.

To configure PyCharm to use the Docker Jupyter server instead of a local Python interpreter, follow these steps:

1. Go to **Settings** ➡️ **Languages & Frameworks** ➡️ **Jupyter** ➡️ **Jupyter Servers**.
2. Select **Configured Server** and click the `+` icon to add an **External Server**.
3. **Name:** `Docker Jupyter Server` (or preferred name).
4. **Server URL:** `http://localhost:8888`
5. **Token:** Retrieve the token from the container logs by running:
   ```bash
   docker compose logs jupyter
   ```
   Look for a line resembling:
   `http://127.0.0.1:8888/lab?token=521936865204d08c50e582...`
<br>
   Copy the alphanumeric token and paste it into the token field.
6. Click **Test Connection** to verify.

> ⚠️ *Important: You will need to copy the token each time you restart the Docker container unless you configure a 
<br> static token in your docker-compose file.*

### 🐍 Set up the Python Interpreter in PyCharm
1. Go to **Settings** ➡️ **Project** ➡️ **Python Interpreter**.
2. Click **Add Interpreter** ➡️ **On Docker Compose...**
3. Select **Service:** `jupyter`.

---

## 🐳 Useful Docker Commands

| Action | Command |
| :--- | :--- |
| **Check Cluster Status** | `docker compose ps` |
| **View Jupyter Logs** | `docker compose logs jupyter` |
| **View Spark Master Logs** | `docker compose logs spark-master` |
| **View All Worker Logs** | `docker compose logs spark-worker` |
| **Stream/Follow Worker Logs** | `docker compose logs -f spark-worker` |
| **Stop the Cluster Safely** | `docker compose down` |
| **Force Stop & Clean Up** | `docker compose down --remove-orphans` |

> 💾 *Note: Your notebooks, applications, and datasets inside `notebooks/`, `apps/`, and `data/` are persistent and 
> <br> will remain on your local computer after stopping the containers.*

---

## 📝 Word Count Example

Word counting is a fundamental text analysis process that calculates the frequency of words in a document. 
<br> It serves as the standard "Hello World" benchmark for distributed data processing frameworks.

### Example Case
* **Input Text:** `"Big data means big opportunities with big challenges."`
* **Expected Output:**
  ```text
  big → 3
  data → 1
  means → 1
  opportunities → 1
  with → 1
  challenges → 1
  ```

### Provided Implementations
Please test and review the following implementations inside the project:

| File Path | Description |
| :--- | :--- |
| `apps/word_count/sequential_word_count.py` | Native, single-threaded sequential Python implementation. |
| `notebooks/word_count.ipynb` | Interactive Jupyter Notebook containing the sequential Python implementation, <br/> the low-level **Spark RDD API** and  the high-level **Spark SQL/DataFrame API**.|

---

## 📚 Official Documentation
* [Docker Compose Documentation](https://docs.docker.com/compose/)
* [Apache Spark Official Documentation](https://spark.apache.org/docs/latest/)
* [Spark RDD Programming Guide](https://spark.apache.org/docs/latest/rdd-programming-guide.html)

