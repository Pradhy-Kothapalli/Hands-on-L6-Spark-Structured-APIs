# Music Streaming Analysis Using Spark Structured APIs

## Overview
This hands-on aims to analyze user music listening behavior and gain some insights, such as each user's favorite genre, Avg listening time, Genre loyalty scores, and users who listen late at night.

## Dataset Description
This hands-on uses two CSV datasets that contain user listening activity and metadata about each song.
## Repository Structure
![Repository Structure](Screenshot%202026-02-25%20091750.png)
## Output Directory Structure
All generated analysis results are stored inside the `outputs/` directory.

The folder contains one CSV file per task:

- `user_favorite_genres.csv`
- `avg_listen_time.csv`
- `top10_genre_loyalty.csv`
- `late_night_users.csv`

## Tasks and Outputs
Task 1 asked for each user's favorite genre. To achieve this, data was grouped by user_id and genre to compute total listen counts and total listening seconds. Genres were then ranked per user based on listen count (with total seconds and alphabetical order as tie-breakers), and the top-ranked genre was selected for each user. Task 2 asked for the average listening time per user. To achieve this, the data was grouped by user_id and the average of duration_sec was computed. The result was rounded to two decimal places and sorted in descending order. Task 3 asked to create and rank a genre loyalty score. To achieve this, total plays per user and plays per genre per user were calculated. A share value (plays_in_genre / total_plays_user) was computed. The results were ranked globally, and the top 10 were selected. Task 4 asked to identify users who listen between 12:00 AM and 5:00 AM. To achieve this, the hour was extracted from the timestamp and filtered accordingly. Late-night play counts, total seconds, and the ratio relative to total user plays were then computed for each user.
## *Prerequisites*

Before starting the assignment, ensure you have the following software installed and properly configured on your machine:

1. *Python 3.x*:
   - [Download and Install Python](https://www.python.org/downloads/)
   - Verify installation:
     ```bash
     python3 --version
     ```

2. *PySpark*:
   - Install using pip:
     ```bash
     pip install pyspark
     ```

3. *Apache Spark*:
   - Ensure Spark is installed. You can download it from the [Apache Spark Downloads](https://spark.apache.org/downloads.html) page.
   - Verify installation by running:
     ```bash
     spark-submit --version
     ```

### *2. Running the Analysis Tasks*

####  *Running Locally*

1. *Generate the Input*:
  ```bash
   python3 input_generator.py
   ```

2. **Execute Each Task Using spark-submit**:
   ```bash
     spark-submit main.py
   ```

3. *Verify the Outputs*:
   Check the outputs/ directory for the resulting files:
   ```bash
   ls outputs/
   ```

## Errors and Resolutions
