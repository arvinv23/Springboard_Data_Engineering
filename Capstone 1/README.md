# Customer Orders Analysis
This project analyzes various datasets related to customer orders, including pet food customer orders, aisles, order products prior, order products train, departments, orders, and products.

## Datasets
The project uses the following datasets stored in CSV files:

1. pet_food_customer_orders.csv: Contains information about pet food customer orders.
   - customer_id
   - pet_id
   - pet_order_number
   - wet_food_order_number
   - orders_since_first_wet_trays_order
   - pet_has_active_subscription
   - pet_food_tier
   - pet_signup_datetime
   - pet_allergen_list
   - pet_fav_flavour_list
   - pet_health_issue_list
   - neutered
   - gender
   - pet_breed_size
   - signup_promo
   - ate_wet_food_pre_tails
   - dry_food_brand_pre_tails
   - pet_life_stage_at_order
   - order_payment_date
   - kibble_kcal
   - wet_kcal
   - total_order_kcal
   - wet_trays
   - wet_food_discount_percent
   - wet_tray_size
   - premium_treat_packs
   - dental_treat_packs
   - wet_food_textures_in_order
   - total_web_sessions
   - total_web_sessions_since_last_order
   - total_minutes_on_website
   - total_minutes_on_website_since_last_order
   - total_wet_food_updates
   - total_wet_food_updates_since_last_order
   - last_customer_support_ticket_date
   - customer_support_ticket_category
2. aisles.csv: Contains information about product aisles.
    - aisle_id
    - aisle
3. order_products__prior.csv: Contains information about prior order products.
  - order_id
  - product_id
  - add_to_cart_order
  - reordered
4. order_products__train.csv: Contains information about order products used for training.
  - order_id
  - product_id
  - add_to_cart_order
  - reordered
5. departments.csv: Contains information about product departments.
  - department_id
  - department
6. orders.csv: Contains information about customer orders.
  - order_id
  - user_id
  - eval_set
  - order_number
  - order_dow
  - order_hour_of_day
  - days_since_prior_order
7. products.csv: Contains information about products.
  - product_id
  - product_name
  - aisle_id
  - department_id

## Dependencies
The following libraries are used in this project:

  - plotly
  - pandas
  - numpy
  - plotly.express
  - plotly.graph_objects
  - holoviews
  - seaborn
  - matplotlib
Make sure to install these libraries before running the code.

## Data Analysis
The Jupyter Notebook performs the following data analysis tasks:

  1. Importing the necessary libraries and initializing the notebook for plotting.
  2. Loading the datasets into pandas DataFrames.
  3. Exploring each dataset by checking its shape, columns, and displaying the first few rows.
  4. Checking for missing values in each column of the DataFrames.
  5. Visualizing the distribution of various features using histograms and bar plots.
  6. Analyzing the relationship between different variables using scatter plots and bar plots.

## Usage
To run the code and reproduce the analysis, follow these steps:

  1. Install the required dependencies listed in the "Dependencies" section.
  2. Download the dataset CSV files and place them in the same directory as the Jupyter Notebook.
  3. Open the Jupyter Notebook and run the cells in sequential order to execute the code and generate the visualizations.
  4. Explore the generated visualizations and analyze the insights provided by the data analysis.

## Conclusion
This project demonstrates how to load, explore, and analyze multiple datasets related to customer orders using Python and various data visualization libraries. By examining the distributions of key variables and their relationships across different datasets, we can gain valuable insights into customer behavior and purchasing patterns.
