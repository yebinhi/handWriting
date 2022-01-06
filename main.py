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
    rows={}
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
            if value['securityId_'] not in rows:
                rows[value['securityId_']] = [0]*10
            temp = rows.get(value['securityId_'])
            temp[0] = value['isin_']
            temp[1] = value['currency_']


        if header['msgType_'] == 12:
            value = json_line['bookEntry_']

            if value['securityId_'] not in rows:
                rows[value['securityId_']] = [0]*10

            temp = rows.get(value['securityId_'])

            if value['side_'] == 'BUY':
                temp[2] = temp[2] + 1
                temp[4] = temp[4] + value['quantity_']
                if temp[8] != 0:
                    temp[8] = max(value['price_'], temp[8])
            elif value['side_'] == 'SELL':
                temp[3] = temp[2] + 1
                temp[5] = temp[5] + value['quantity_']
                if temp[9] != 0:
                    temp[9] = min(value['price_'], temp[9])
    return rows

def cal_weight_buy_sell(rows):
    for key, value in rows.items():
        if value[4] != 0:
            value[6] = value[2] / value[4]
        if value[5] != 0:
            value[7] = value[3]/value[5]
    return rows


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
    output_df = pd.DataFrame(columns =get_column_list())
    rows = retrive_data('pretrade_current.txt')
    rows = cal_weight_buy_sell(rows)
    generate_out_put(rows.values(), 'output2.csv')
    print(rows.values())

