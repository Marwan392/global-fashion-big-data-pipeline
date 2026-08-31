import sys
sys.path.insert(0, "/opt/spark/apps")
from spark.cleaning import clean_all
from spark.transformation import transform_all ,fact_df

print(fact_df.show(5, truncate=False))