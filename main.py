import csv
import json
import re
import sys
import pandas as pd


def get_column_list():
    column_list = ['ISIN', 'Currency', 'Total Buy Count', 'Total Sell Count',
                   'Total Buy Quantity', 'Total Sell Quantity', 'Weighted Average Buy Price',
                   'Weighted Average Sell Price', 'Max Buy Price', 'Min Sell Price']
    return column_list


def remove_at(i, s):
    return s[:i] + s[i + 1:]


def insert_into(source_str, insert_str, pos):
    return source_str[:pos] + insert_str + source_str[pos:]


def pre_process(s):
    while True:
        try:
            if s == '': return ''
            result = json.loads(s)  # try to parse...
            break  # parsing worked -> exit loop
        except Exception as e:
            message = e.msg

            if message == 'Extra data':
                s = remove_at(e.pos - 1, s)
            elif message == 'Expecting property name enclosed in double quotes' and s[0:2] == '{{':
                s = insert_into(s, '"header":', e.pos)
            elif message == 'Expecting \',\' delimiter' and s[e.pos - 12:e.pos] == '"flags_":"{"':
                s = remove_at(e.pos - 3, s)
            elif message == 'Expecting value' and s[e.pos: e.pos + 3] == 'BUY':
                s = insert_into(s, '"', e.pos)
                s = insert_into(s, '"', e.pos + 4)
            elif message == 'Expecting value' and s[e.pos: e.pos + 4] == 'SELL':
                s = insert_into(s, '"', e.pos)
                s = insert_into(s, '"', e.pos + 5)
            else:
                print(message)
                print(s)

    return result


def retrive_data(path):
    SecurityReference = []
    orders = []
    with open(path, 'r') as f:
        lines = f.readlines()

    count = 0
    for line in lines:
        count += 1
        print(count)
        if re.match(r'[0-9]* \(.*\) [0-9]', line):
            continue

        json_line = pre_process(line)
        header = json_line['header']

        if header['msgType_'] == 8:
            value = json_line['security_']
            SecurityReference.append([value['isin_'],
                                      value['currency_'],
                                      value['securityId_']])

        if header['msgType_'] == 12:
            value = json_line['bookEntry_']
            orders.append([value['securityId_'],
                           value['side_'],
                           value['quantity_'],
                           value['price_']])

    return [SecurityReference, orders]


def generate_out_put(data, out_path):
    # generate output file
    header = get_column_list()

    with open(out_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, delimiter = "|")
        writer.writerow(header)
        writer.writerows(data)
    csvfile.close()


if __name__ == '__main__':
    output = []
    [SecurityReference, orders] = retrive_data('pretrade_current.txt')

    # -----------------------------------------
    # use panda data frame
    # -----------------------------------------
    df = pd.DataFrame(orders)
    count = 0
    for item in SecurityReference:
        count += 1
        print(count)
        id = item[2]
        # total buy and
        total_buy = df.loc[(df[0] == id) & (df[1] == 'BUY'), :].shape[0]
        if total_buy <= 0:
            continue
        # total sell
        total_sell = df.loc[(df[0] == id) & (df[1] == 'SELL'), :].shape[0]
        if total_sell <= 0:
            continue

        # Total Buy Quantity
        Total_Buy_Quantity = df.loc[(df[0] == id) & (df[1] == 'BUY'), 2:2].sum()

        # Total Sell Quantity
        Total_Sell_Quantity = df.loc[(df[0] == id) & (df[1] == 'SELL'), 2:2].sum()

        # Max Buy Price .max()
        Max_Buy_Price = df.loc[(df[0] == id) & (df[1] == 'BUY'), 3:3].max()
        if Max_Buy_Price.shape[0] <= 0: continue

        # Min Sell Price
        Min_Sell_Price = df.loc[(df[0] == id) & (df[1] == 'SELL'), 3:3].min()

        # print(Min_Sell_Price.loc[0])
        if Min_Sell_Price.shape[0] <= 0: continue

        # Weighted Average BUY Price
        Weighted_Average_BUY_Price = total_buy / Total_Buy_Quantity

        # Weighted Average sell Price
        Weighted_Average_SELL_Price = total_sell / Total_Sell_Quantity

        # produce output
        vec = [item[0], item[1], str(total_buy), str(total_sell),
               str(Total_Buy_Quantity.iloc[0]),
               str(Total_Sell_Quantity.iloc[0]),
               str(round(Weighted_Average_BUY_Price.iloc[0], 8)),
               str(round(Weighted_Average_SELL_Price.iloc[0],8)),
               str(Max_Buy_Price.iloc[0]),
               str(Min_Sell_Price.iloc[0])]
        output.append(vec)

    # generate output file
    generate_out_put(output, 'output.csv')
