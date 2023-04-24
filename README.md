# Dynamic Window Configuration Prediction

The main.py script has two main tasks:

    1. The get_data function which collects liquidity data BigQuery every 12 hours.

    2. The update_window function which updates the zookeeper window config every 10 mins.

get_data

    1. Collect liquidity data (MM starts and number of users) of the last 24 hours from Bigquery.

    2. Predict liquidity every 10 minutes as the liquidity at the same time last day.

    3. Store the liquidity predictions.

update_window

    1. Fetch the liquidity prediction data for the current time.

    2. Feed the prediction to the neural network model along with our targets for zeros and MM time.

    3. The targets for zeros, P25, P50, P75, P90, P95 are currently 15%, 3s, 6s, 9s, 10s, 11s respectively.

    4. The model then predicts a window configuration based on the liquidity.

    5. Update the predicted window configuration on zookeeper.
    
    6. Repeat this every 10 mins.

The current implementation is only for Fruitfight Rs. 1 table.