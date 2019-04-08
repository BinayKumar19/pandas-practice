import copy
import pandas as pd
import mysql.connector as sql


def part2(fare_classes_df):
    total_class_booking = fare_classes_df.groupby('FlightId')['Booking'].sum()
    print(total_class_booking)

    dataset = pd.merge(fare_classes_df, total_class_booking, on='FlightId')
    dataset['Percentage'] = (dataset.loc[:, 'Booking_x'] / dataset.loc[:, 'Booking_y']).round(2)

    results = dataset[['FlightId', 'FareClass', 'FareClassRank', 'Booking_x', 'Percentage']]
    print(results)


def comparison(x):
    res = copy.deepcopy(x)
    for i in range(0, len(x) - 1):
        res[i:] = (x.iloc[i] / x.iloc[i + 1]).round(2)
    x = res
    x[len(x) - 1:] = 0

    return x


def part1(fare_classes_df):
    fare_classes_df['Ratio'] = fare_classes_df.groupby('FlightId')['FareValue'].apply(comparison)
    print(fare_classes_df)


db_connection = sql.connect(host='localhost', database='practice', user='binay', password='123456')
sql_query = 'SELECT * FROM t_fare_classes'

fare_classes_df = pd.read_sql_query(sql_query, con=db_connection)

part1(fare_classes_df)
part2(fare_classes_df)