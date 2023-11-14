# spark-plumber2.R

library(sparklyr); library(dplyr)

sc <- spark_connect(master = "local", version = "3.4.0") 

spark_model2 <- ml_load(sc, path = "spark_model2")

#* @post /predict 

function(Recency, Frequency, Monetary) {
  new_data <- data.frame(
    Recency = as.numeric(Recency),
    Frequency = as.numeric(Frequency), 
    Monetary = as.numeric(Monetary)
  )
  
  new_data_r <- copy_to(sc, new_data, overwrite = TRUE)
  
  ml_transform(spark_model2, new_data_r) |>
    pull(cluster)

}