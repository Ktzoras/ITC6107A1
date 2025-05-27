from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from json import loads, dumps
from time import sleep


STOCK_TO_PARTITION = {
    'IBM': 0, 'AAPL': 1, 'FB': 2, 'AMZN': 3, 'GOOG': 4, 'META': 5, 'MSI': 6,
    'INTC': 7, 'AMD': 8, 'MSFT': 9, 'DELL': 10, 'ORKL': 11,
    'HPQ': 12, 'CSCO': 13, 'ZM': 14, 'QCOM': 15, 'ADBE': 16, 'VZ': 17,
    'TXN': 18, 'CRM': 19, 'AVGO': 20, 'NVDA': 21, 'MSTR': 22, 'EBAY': 23
}

portfolios = {
    "P11": {"IBM": 1300, "AAPL": 2200, "FB": 1900, "AMZN": 2500, "GOOG": 1900, "AVGO": 2400},
    "P12": {"VZ": 2900, "INTC": 2600, "AMD": 2100, "MSFT": 1200, "DELL": 2700, "ORKL": 1200}
}

relevant_stocks = ['IBM', 'AAPL', 'FB', 'AMZN', 'GOOG', 'AVGO', 'VZ', 'INTC', 'AMD', 'MSFT', 'DELL', 'ORKL']

start_stocks = [
    ('IBM', 256.90), ('AAPL', 227.48), ('FB', 597.99), ('AMZN', 194.54),
    ('GOOG', 167.81), ('META', 597.99), ('MSI', 415.67), ('INTC', 19.93),
    ('AMD', 96.63), ('MSFT', 380.16), ('DELL', 90.34), ('ORKL', 148.79),
    ('HPQ', 28.98), ('CSCO', 60.06), ('ZM', 73.47), ('QCOM', 154.98),
    ('ADBE', 435.08), ('VZ', 46.49), ('TXN', 186.49), ('CRM', 272.90),
    ('AVGO', 184.45), ('NVDA', 106.98), ('MSTR', 239.27), ('EBAY', 68.19)
    ]

partitions = [
    TopicPartition('StockExchange', STOCK_TO_PARTITION[s])
    for s in relevant_stocks if s in STOCK_TO_PARTITION
]



consumer = KafkaConsumer(
    group_id = 'investor1',
    bootstrap_servers = ['localhost:9999'],
    value_deserializer = lambda x: loads(x.decode('utf-8')),
    key_deserializer = lambda k: k.decode('utf-8') if k else None,
    enable_auto_commit = True,
    auto_offset_reset = 'earliest'
)

producer = KafkaProducer(
    bootstrap_servers = ['localhost:9999'],
    value_serializer = lambda x: dumps(x).encode('utf-8'),
    key_serializer = lambda k: k.encode('utf-8')
)


def get_prices(part_list):
    consumer.assign(part_list)
    for msg in consumer:
        yield msg.value


def main():
    position_start = {portfolio: {stock: 0 for stock in portfolios[portfolio].keys()} for portfolio in portfolios.keys()}
    for stock in start_stocks:
        if stock[0] in position_start['P11'].keys():
            position_start['P11'][stock[0]] = stock[1]*portfolios['P11'][stock[0]]
        elif stock[0] in position_start['P12'].keys():
            position_start['P12'][stock[0]] = stock[1]*portfolios['P12'][stock[0]]
        else:
            pass
    
    yesterday_total_NAV_1 = 0
    yesterday_total_NAV_2 = 0
    FLAG_start = True

    while True:
        todays_total_NAV_1 = 0
        todays_total_NAV_2 = 0
        for part in partitions:
            price_generator = get_prices([part])
            stocks = next(price_generator)
            try:    
                todays_total_NAV_1 += portfolios['P11'][stocks['tick']]*stocks['price']
            except:
                todays_total_NAV_2 += portfolios['P12'][stocks['tick']]*stocks['price']

            if FLAG_start:
              try:
                yesterday_total_NAV_1 += position_start['P11'][stocks['tick']]
              except:
                yesterday_total_NAV_2 += position_start['P12'][stocks['tick']]
        Flag_start = False

        diff_NAV_1 = todays_total_NAV_1 - yesterday_total_NAV_1
        diff_NAV_2 = todays_total_NAV_2 - yesterday_total_NAV_2
        
        percent_diff_1 = round((diff_NAV_1)/yesterday_total_NAV_1 * 100, 2)
        percent_diff_2 = round((diff_NAV_2)/yesterday_total_NAV_2 * 100, 2)

        yesterday_total_NAV_1 = todays_total_NAV_1
        yesterday_total_NAV_2 = todays_total_NAV_2

        producer.send(
            'portfolios',
            key = stocks['ts'],
            value = {
                'investor': 'Inv1',
                'portfolio': 'P11',
                'As_Of': stocks['ts'],
                'NAV_per_Share': todays_total_NAV_1,
                'Daily_NAV_Change': diff_NAV_1,
                'Daily_NAV_Change_Percent': percent_diff_1
                },         
            partition = 0
            )

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


if __name__ == '__main__':
    main()