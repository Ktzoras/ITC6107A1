from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from json import loads, dumps
from time import sleep
from datetime import datetime, timedelta

# Dictionary with key-values of Stock tickers and partition numbers
# following the Stock Exchange server setup
# Used to filter for the correct partitions
# in combination with the relevant_stocks list
STOCK_TO_PARTITION = {
    'IBM': 0, 'AAPL': 1, 'FB': 2, 'AMZN': 3, 'GOOG': 4, 'META': 5, 'MSI': 6,
    'INTC': 7, 'AMD': 8, 'MSFT': 9, 'DELL': 10, 'ORCL': 11,
    'HPQ': 12, 'CSCO': 13, 'ZM': 14, 'QCOM': 15, 'ADBE': 16, 'VZ': 17,
    'TXN': 18, 'CRM': 19, 'AVGO': 20, 'NVDA': 21, 'MSTR': 22, 'EBAY': 23
}

# Dictionary containing the portfolios of the respective investor
portfolios = {
    "P11": {"IBM": 1300, "AAPL": 2200, "FB": 1900, "AMZN": 2500, "GOOG": 1900, "AVGO": 2400},
    "P12": {"VZ": 2900, "INTC": 2600, "AMD": 2100, "MSFT": 1200, "DELL": 2700, "ORCL": 1200}
}

# List containing only the tickers of the stocks 
relevant_stocks = ['IBM', 'AAPL', 'FB', 'AMZN', 'GOOG', 'AVGO', 'VZ', 'INTC', 'AMD', 'MSFT', 'DELL', 'ORCL']

# Dictionary with initial prices of all stocks
start_stocks = [
    ('IBM', 256.90), ('AAPL', 227.48), ('FB', 597.99), ('AMZN', 194.54),
    ('GOOG', 167.81), ('META', 597.99), ('MSI', 415.67), ('INTC', 19.93),
    ('AMD', 96.63), ('MSFT', 380.16), ('DELL', 90.34), ('ORCL', 148.79),
    ('HPQ', 28.98), ('CSCO', 60.06), ('ZM', 73.47), ('QCOM', 154.98),
    ('ADBE', 435.08), ('VZ', 46.49), ('TXN', 186.49), ('CRM', 272.90),
    ('AVGO', 184.45), ('NVDA', 106.98), ('MSTR', 239.27), ('EBAY', 68.19)
    ]

# Listing the topic partitions {kafka.TopicPartition()} relevant for this investor
partitions = [
    TopicPartition('StockExchange', STOCK_TO_PARTITION[s]) # Retrieving the partition number from the the dictionary
    for s in relevant_stocks # iterating through the list with all the stocks of this investor
    if s in STOCK_TO_PARTITION # filtering for stocks in the SE topic to avoid errors when indexing but redundant in our case
]


# Creating the consumer for this investor
consumer = KafkaConsumer(
    group_id = 'investor1', # 1 group per investor to avoid offset issues
    bootstrap_servers = ['localhost:9999'], # listening to port 9999 as requested
    value_deserializer = lambda x: loads(x.decode('utf-8')), # json and format decoder
    key_deserializer = lambda k: k.decode('utf-8') if k else None,
    enable_auto_commit = True, # the consumer’s offset will be periodically committed in the background
    auto_offset_reset = 'earliest' # ‘earliest’ will move to the oldest available message
)                                  # allows the consumer to grab the dates as FIFO, making it keep the dates consistent

# Creating the producer for this investor
producer = KafkaProducer(
    bootstrap_servers = ['localhost:9999'], # sending to port 9999 as requested
    value_serializer = lambda x: dumps(x).encode('utf-8'),
    key_serializer = lambda k: k.encode('utf-8')
)

# Generator grabs and yields the first available message value
# found on the specified partition
def get_prices(se_topic_partition):
    consumer.assign(se_topic_partition) # assign specified partition
    for msg in consumer: # grabs first available
        yield msg.value  # yields the value

# check for weekend dates
def is_weekend(date):
    return date.weekday() >= 5



def main():

    # starting up the position of this investor. Needed for calculating the 
    # difference and % diff, after retrieving the first dates prices

    # creating an empty dictionary structured according to this investor
    position_start = {
        portfolio: {stock: 0 for stock in portfolios[portfolio].keys()} 
        for portfolio in portfolios.keys()
        }

    # calculating the position for each stock in the dictionary
    for stock in start_stocks: # by iterating through initial stocks-prices
        if stock[0] in position_start['P11'].keys(): # checking if that stock is part of this portfolio
            position_start['P11'][stock[0]] = stock[1]*portfolios['P11'][stock[0]] # calculating the position
        if stock[0] in position_start['P12'].keys(): # checking if that stock is part of this portfolio
            position_start['P12'][stock[0]] = stock[1]*portfolios['P12'][stock[0]] # calculating the position
        else: # if not part of any 
            pass # skip the stock

    # Initiallizing values used later
    # Nav and a flag used for the first iteration
    yesterday_total_NAV_1 = 0 
    yesterday_total_NAV_2 = 0
    FLAG_start = True
    # the date, following the server setup
    current_date = datetime(2000, 1, 1)
    end_date = datetime(2025, 3, 26)
    # a dictionary used for stocks received on 
    # dates that don't match "today's" date
    saved_data = {}

    while True: # main loop
        # Initializing today's values
        # populated later
        todays_total_NAV_1 = 0 
        todays_total_NAV_2 = 0

        # checking if today is weekend
        if is_weekend(current_date):
            current_date += timedelta(days=1) # adding 1 day to make current_date tomorrow
            continue # skipping if weekend 


        # if today week day we start iterating through partition
        for part in partitions:
            price_generator = get_prices([part])
            stocks = next(price_generator) # generating first available stock data

            # transformation to be used for checking if today
            stocks_date = datetime.strptime(stocks['ts'], "%d-%b-%y")

            if stocks_date == current_date: # checking if we received today's stock prices

              # if yes, we populate for each stock
                if stocks['tick'] in portfolios['P11'].keys(): # checking if that stock is part of this portfolio
                    todays_total_NAV_1 += portfolios['P11'][stocks['tick']]*stocks['price'] # adding to today's NAV
                if stocks['tick'] in portfolios['P12'].keys(): # checking if that stock is part of this portfolio
                    todays_total_NAV_2 += portfolios['P12'][stocks['tick']]*stocks['price'] # adding to today's NAV


            # If we didn't receive today's stock prices
            else: # we save what we receive in the initialized dictionary
                if stocks_date not in saved_data: # if first time saving for this date
                    saved_data[stocks_date] = {} # initialize for this date
                saved_data[stocks_date][stocks['tick']] = stocks['price'] # save the price for this stock ticker

                try: # need to check if today's stock price is already saved
                    price = saved_data[current_date][stocks['tick']] # grabing price
                    saved_data[current_date].pop(stocks['tick']) # removing the price

                    # populate for each stock
                    if stocks['tick'] in portfolios['P11'].keys():
                        todays_total_NAV_1 += portfolios['P11'][stocks['tick']]*price
                    if stocks['tick'] in portfolios['P12'].keys():
                        todays_total_NAV_2 += portfolios['P12'][stocks['tick']]*price

                except: # if not
                  continue # we skip the rest and retrieve from next partition


          # initializg the first position using the partition iterator and retrieved tickers
            if FLAG_start:
                if stocks['tick'] in position_start['P11'].keys():
                    yesterday_total_NAV_1 += position_start['P11'][stocks['tick']]
                if stocks['tick'] in position_start['P12'].keys():
                    yesterday_total_NAV_2 += position_start['P12'][stocks['tick']]

        # switching off the above check
        Flag_start = False

        # calculating the difference in NAV for each portfolio
        diff_NAV_1 = todays_total_NAV_1 - yesterday_total_NAV_1
        diff_NAV_2 = todays_total_NAV_2 - yesterday_total_NAV_2

        # fail-safe for division by 0
        if yesterday_total_NAV_1 == 0:
            percent_diff_1 = 0
        else:
            percent_diff_1 = round((diff_NAV_1)/yesterday_total_NAV_1 * 100, 2)

        if yesterday_total_NAV_2 == 0:
            percent_diff_2 = 0
        else:
            percent_diff_2 = round((diff_NAV_2)/yesterday_total_NAV_2 * 100, 2)

        # saving the today's values for the next iteration/date (for 'tomorrow')
        yesterday_total_NAV_1 = todays_total_NAV_1
        yesterday_total_NAV_2 = todays_total_NAV_2

        # sending for each portfolio
        producer.send(
            'portfolios',
            key = stocks['ts'],
            value = { # sending a single JSON with all info for this portfolio's position today
                'investor': 'Inv1',
                'portfolio': 'P11',
                'As_Of': stocks['ts'],
                'NAV_per_Share': todays_total_NAV_1,
                'Daily_NAV_Change': diff_NAV_1,
                'Daily_NAV_Change_Percent': percent_diff_1
                },
            partition = 0 # partition used by this investor
            )
        # sending for the other portfolio
        producer.send(
            'portfolios',
            key = stocks['ts'],
            value = {
                'investor': 'Inv1',
                'portfolio': 'P12',
                'As_Of': stocks['ts'],
                'NAV_per_Share': todays_total_NAV_2,
                'Daily_NAV_Change': diff_NAV_2,
                'Daily_NAV_Change_Percent': percent_diff_2
                },
            partition = 0
            )

        # trying to retrieve prices for dates that where not yet sent 
        try:
          for date in saved_data.keys(): # this may give an error and break if not in try:
            for tick in relevant_stocks: # iterating through all relevant stocks

              # populating todays nav as above
              if tick in portfolios['P11'].keys(): 
                todays_total_NAV_1 += portfolios['P11'][tick]*saved_data[date][tick]
              if tick in portfolios['P12'].keys():
                todays_total_NAV_2 += portfolios['P12'][tick]*saved_data[date][tick]

            diff_NAV_1 = todays_total_NAV_1 - yesterday_total_NAV_1
            diff_NAV_2 = todays_total_NAV_2 - yesterday_total_NAV_2

            if yesterday_total_NAV_1 == 0:
                percent_diff_1 = 0
            else:
                percent_diff_1 = round((diff_NAV_1)/yesterday_total_NAV_1 * 100, 2)

            if yesterday_total_NAV_2 == 0:
                percent_diff_2 = 0
            else:
                percent_diff_2 = round((diff_NAV_2)/yesterday_total_NAV_2 * 100, 2)

            yesterday_total_NAV_1 = todays_total_NAV_1
            yesterday_total_NAV_2 = todays_total_NAV_2

            producer.send(
                'portfolios',
                key = date,
                value = {
                    'investor': 'Inv1',
                    'portfolio': 'P11',
                    'As_Of': date,
                    'NAV_per_Share': todays_total_NAV_1,
                    'Daily_NAV_Change': diff_NAV_1,
                    'Daily_NAV_Change_Percent': percent_diff_1
                    },
                partition = 0
                )
                
            producer.send(
                'portfolios',
                key = date,
                value = {
                    'investor': 'Inv1',
                    'portfolio': 'P12',
                    'As_Of': date,
                    'NAV_per_Share': todays_total_NAV_2,
                    'Daily_NAV_Change': diff_NAV_2,
                    'Daily_NAV_Change_Percent': percent_diff_2
                    },
                partition = 0
                )

            # after sending for that date we missed
            saved_data.pop(date) # we remove it from the dictionary
        except: # and when the saved_data dict is empty and we get an error
          pass # we move to tomorrow

        current_date += timedelta(days = 1) # tomorrow


if __name__ == '__main__':
    main()