import pandas as pd
import mysql.connector as sql


def operation_in_pandas():
    fare_sql_query = 'SELECT * FROM t_fare_info'

    flight_sql_query = 'SELECT * FROM t_flight_info'

    fare_df = pd.read_sql_query(fare_sql_query, con=db_connection)
    flight_df = pd.read_sql_query(flight_sql_query, con=db_connection)

    dataset = pd.merge(fare_df, flight_df, on='FlightId')

    dataset['Bookings_Q'] = ((dataset.loc[:, 'Bookings_Q'] / 100) * dataset.loc[:, 'total_booking']).round().astype(int)
    dataset['Bookings_X'] = ((dataset.loc[:, 'Bookings_X'] / 100) * dataset.loc[:, 'total_booking']).round().astype(int)
    dataset['Bookings_V'] = ((dataset.loc[:, 'Bookings_V'] / 100) * dataset.loc[:, 'total_booking']).round().astype(int)
    dataset['Bookings_Y'] = ((dataset.loc[:, 'Bookings_Y'] / 100) * dataset.loc[:, 'total_booking']).round().astype(int)

    results = pd.DataFrame()
    results[['FlightId', 'total_booking']] = dataset[['FlightId', 'total_booking']]
    results[['Q', 'X', 'V', 'Y']] = dataset[['Bookings_Q', 'Bookings_X', 'Bookings_V', 'Bookings_Y']]

    results['sum'] = dataset.loc[:, 'Bookings_Q'] + dataset.loc[:, 'Bookings_X'] + dataset.loc[:,
                                                                                   'Bookings_V'] + dataset.loc[:,
                                                                                                   'Bookings_Y']
    print(results)


def operation_in_sql():
    sql_query = 'SELECT res.FlightId, res.total_booking, FORMAT(res.Q,0) Q, FORMAT(res.X,0) X, ' \
                'FORMAT(res.V,0) V, FORMAT(res.Y,0) Y, FORMAT((res.Q+res.X+res.V + res.Y),0) sum ' \
                'FROM (SELECT fare.FlightId, ' \
                'TRUNCATE((Bookings_Q/100)*total_booking,0) Q, ' \
                'TRUNCATE((Bookings_X/100)*total_booking,0) X, ' \
                'TRUNCATE((Bookings_V/100)*total_booking,0) V, ' \
                'TRUNCATE((Bookings_Y/100)*total_booking,0) Y, ' \
                'total_booking ' \
                'FROM t_fare_info fare INNER JOIN t_flight_info flight ON  fare.FlightId = flight.FlightId) res'

    df = pd.read_sql_query(sql_query, con=db_connection)

    print(df)


db_connection = sql.connect(host='localhost', database='practice', user='binay', password='123456')

print('main operation is performed in a Pandas Dataframe')
operation_in_pandas()

print('\nmain operation is performed in a MySql Query')
operation_in_sql()
