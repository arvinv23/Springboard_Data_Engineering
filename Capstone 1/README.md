Pet Food Customer Orders Analysis
This project analyzes a dataset of pet food customer orders to gain insights into customer behavior and purchasing patterns.

Dataset
The dataset is stored in a CSV file named pet_food_customer_orders.csv. It contains the following columns:

customer_id
pet_id
pet_order_number
wet_food_order_number
orders_since_first_wet_trays_order
pet_has_active_subscription
pet_food_tier
pet_signup_datetime
pet_allergen_list
pet_fav_flavour_list
pet_health_issue_list
neutered
gender
pet_breed_size
signup_promo
ate_wet_food_pre_tails
dry_food_brand_pre_tails
pet_life_stage_at_order
order_payment_date
kibble_kcal
wet_kcal
total_order_kcal
wet_trays
wet_food_discount_percent
wet_tray_size
premium_treat_packs
dental_treat_packs
wet_food_textures_in_order
total_web_sessions
total_web_sessions_since_last_order
total_minutes_on_website
total_minutes_on_website_since_last_order
total_wet_food_updates
total_wet_food_updates_since_last_order
last_customer_support_ticket_date
customer_support_ticket_category
Dependencies
The following libraries are used in this project:

plotly
pandas
numpy
plotly.express
plotly.graph_objects
holoviews
seaborn
matplotlib
Make sure to install these libraries before running the code.

Data Analysis
The Jupyter Notebook performs the following data analysis tasks:

Importing the necessary libraries and initializing the notebook for plotting.
Loading the pet food customer orders dataset into a pandas DataFrame.
Exploring the dataset by checking its shape, columns, unique customer IDs, unique pet IDs, and displaying the first few rows.
Checking for missing values in each column of the DataFrame.
Visualizing the distribution of various features using histograms and bar plots, including:
Total order kcal
Total minutes on website
Total minutes on website since last order
Total web sessions
Analyzing the relationship between different variables using scatter plots and bar plots, such as:
Total order kcal vs. Total web sessions
Total web sessions vs. Total minutes on website
Total web sessions since last order vs. Total minutes on website since last order
Visualizations
The notebook includes several visualizations to explore and understand the data:

Histogram of total order kcal
Histogram of total minutes on website
Histogram of total minutes on website since last order
Histogram of total web sessions
Scatter plot of total order kcal vs. total web sessions
Bar plot of total web sessions vs. total minutes on website
Bar plot of total web sessions since last order vs. total minutes on website since last order
Usage
To run the code and reproduce the analysis, follow these steps:

Install the required dependencies listed in the "Dependencies" section.
Download the pet_food_customer_orders.csv dataset and place it in the same directory as the Jupyter Notebook.
Open the Jupyter Notebook and run the cells in sequential order to execute the code and generate the visualizations.
Explore the generated visualizations and analyze the insights provided by the data analysis.
Conclusion
This project demonstrates how to load, explore, and analyze a pet food customer orders dataset using Python and various data visualization libraries. By examining the distributions of key variables and their relationships, we can gain valuable insights into customer behavior and purchasing patterns.
