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
            elif message == 'Expecting value' and s[e.pos: e.pos+3] == 'BUY':
                s=insert_into(s, '"', e.pos)
                s=insert_into(s, '"', e.pos + 4)
            elif message == 'Expecting value' and s[e.pos: e.pos+4] == 'SELL':
                s=insert_into(s, '"', e.pos)
                s=insert_into(s, '"', e.pos + 5)
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

        jsonLine = pre_process(line)
        header = jsonLine['header']

        if(header['msgType_'] == 8):
            value = jsonLine['security_']
            SecurityReference.append([value['isin_'],
                                      value['currency_'],
                                      value['securityId_']])

        if(header['msgType_'] == 12):
            value = jsonLine['bookEntry_']
            orders.append([value['securityId_'],
                           value['side_'],
                           value['quantity_'],
                           value['price_']])

    return [SecurityReference, orders]


def generate_out_put(out_path):
    # generate output file
    pass

if __name__ == '__main__':
    output = []
    [SecurityReference, orders] = retrive_data('pretrade_current.txt')

    # -----------------------------------------
    # use panda data frame
    # -----------------------------------------
    df = pd.DataFrame(orders)
    for item in SecurityReference:
        id = item[2]
        # total buy and
        total_buy = df.loc[df[0] == id, df[1] == 'BUY'].shape[0]
        # total sell
        total_sell = df.loc[df[0] == id, df[1] == 'SELL'].shape[0]
        # Total Buy Quantity
        Total_Buy_Quantity = df.loc[(df[0] == id) & (df[1] == 'BUY'), 2:2].sum()
        # Total Sell Quantity
        Total_Sell_Quantity = df.loc[(df[0] == id) & (df[1] == 'SELL'), 3:3].sum()
        # Max Buy Price .max()
        Max_Buy_Price = df.loc[(df[0] == id) & (df[1] == 'BUY'), 2:2].max()
        # Min Sell Price
        Min_Sell_Price = df.loc[(df[0] == id) & (df[1] == 'SELL'), 3:3].min()
        item.append(total_buy,total_sell, Total_Buy_Quantity, Total_Sell_Quantity, Max_Buy_Price, Min_Sell_Price)
        output.append(item)
        # sys.exit()

    # generate output file
    generate_out_put('output.csv')

