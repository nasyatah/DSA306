# spark-plumber.R

library(sparklyr); library(dplyr)

sc <- spark_connect(master = "local", version = "3.4.0") 

spark_model1 <- ml_load(sc, path = "spark_model1")

#* @post /predict 

function(Recency, Frequency, Monetary, Average_basket_size, Unique_products, festive_spender) {
new_data <- data.frame(
  Recency = as.numeric(Recency),
  Frequency = as.numeric(Frequency), 
  Monetary = as.numeric(Monetary),
  Average_basket_size = as.numeric(Average_basket_size),
  Unique_products = as.numeric(Unique_products),
  festive_spender = festive_spender,
  logistic_duration = NA
)
new_data_r <- copy_to(sc, new_data, overwrite = TRUE)

ml_transform(spark_model1, new_data_r) |>
  pull(prediction)

}