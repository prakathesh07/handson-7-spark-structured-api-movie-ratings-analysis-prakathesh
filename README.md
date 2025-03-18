# Movie Ratings Analysis

## **Prerequisites**

Before starting the assignment, ensure you have the following software installed and properly configured on your machine:

1. **Python 3.x**:
   - [Download and Install Python](https://www.python.org/downloads/)
   - Verify installation:
     ```bash
     python3 --version
     ```

2. **PySpark**:
   - Install using `pip`:
     ```bash
     pip install pyspark
     ```

3. **Apache Spark**:
   - Ensure Spark is installed. You can download it from the [Apache Spark Downloads](https://spark.apache.org/downloads.html) page.
   - Verify installation by running:
     ```bash
     spark-submit --version
     ```

4. **Docker & Docker Compose** (Optional):
   - If you prefer using Docker for setting up Spark, ensure Docker and Docker Compose are installed.
   - [Docker Installation Guide](https://docs.docker.com/get-docker/)
   - [Docker Compose Installation Guide](https://docs.docker.com/compose/install/)

## **Overview**

This project leverages PySpark's Structured APIs to analyze a dataset containing movie ratings, user engagement, and streaming behavior. The analysis provides insights into:

- Binge-watching patterns by age group
- Churn risk users
- Movie-watching trends over the years

## **Project Structure**
```
MovieRatingsAnalysis/
├── input/
│   └── movie_ratings_data.csv
├── outputs/
│   ├── binge_watching_patterns.csv
│   ├── churn_risk_users.csv
│   └── movie_watching_trends.csv
├── src/
│   ├── task1_binge_watching_patterns.py
│   ├── task2_churn_risk_users.py
│   └── task3_movie_watching_trends.py
├── docker-compose.yml (optional)
└── README.md
```

## **Technology Stack**
- Python 3.x
- PySpark
- Apache Spark

## **How to Run the Project**

### **1. Local Execution**
```bash
cd MovieRatingsAnalysis/
spark-submit src/task1_binge_watching_patterns.py
spark-submit src/task2_churn_risk_users.py
spark-submit src/task3_movie_watching_trends.py
```

### **2. Docker (Optional)**
```bash
docker-compose up -d
docker exec -it spark-master bash
cd /opt/bitnami/spark/
spark-submit src/task1_binge_watching_patterns.py
spark-submit src/task2_churn_risk_users.py
spark-submit src/task3_movie_watching_trends.py
exit
docker-compose down
```

## **Dataset**
The dataset is present in the `input` folder, saved as `movie_ratings_data.csv`.

### **Example Data**
```
UserID,MovieID,MovieTitle,Genre,Rating,ReviewCount,WatchedYear,UserLocation,AgeGroup,StreamingPlatform,WatchTime,IsBingeWatched,SubscriptionStatus
1,304,The Matrix,Sci-Fi,2.1,21,2019,US,Senior,Disney+,65,False,Canceled
2,998,Interstellar,Sci-Fi,4.8,36,2019,UK,Senior,Amazon,92,True,Canceled
3,684,The Dark Knight,Action,2.7,4,2019,Canada,Senior,Netflix,118,True,Active
...
```

## **Task Outputs**

### **Task 1: Binge-Watching Patterns by Age Group**
| AgeGroup | BingeWatchers | Percentage |
|----------|--------------|------------|
| Adult    | 14           | 37.84%     |
| Senior   | 11           | 32.35%     |
| Teen     | 14           | 48.28%     |

### **Task 2: Churn Risk Users**
| Churn Risk Users | Total Users |
|------------------|-------------|
| Users with low watch time & canceled subscriptions | 20 |

### **Task 3: Movie-Watching Trends by Year**
| WatchedYear | MoviesWatched |
|------------|--------------|
| 2018       | 13           |
| 2019       | 20           |
| 2020       | 18           |
| 2021       | 16           |
| 2022       | 13           |
| 2023       | 20           |

## **Key Insights**
- Teenagers binge-watch the most (**48.28%**).
- **20 users** are identified as high-risk churn (canceled + <100 min watch time).
- **2019 and 2023** were peak years for movie watching, with **20 movies watched** each year.
