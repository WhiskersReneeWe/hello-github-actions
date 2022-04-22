import json
from typing import List
# import emoji

import requests
# from requests.auth import HTTPBasicAuth
# from pprint import pprint
# #from fuzzywuzzy import process
# from bigeye_sdk.datawatch_client import CoreDatawatchClient
# from bigeye_sdk.model.api_credentials import BasicAuthRequestLibApiConf


##### utils functions
def get_table_columns(table_id: int, auth):
    url = "https://app.bigeye.com/api/v1/columns?tableId=" + str(table_id)
    response = requests.get(url, auth=auth)
    res = json.loads(response.text)

    columns = {}
    for i in res['columns']:
        columns[i['id']] = i['name']

    return columns


def get_table_id(table_name, auth):
    URL_get_tableid = "https://app.bigeye.com/api/v1/tables?tableName=" + table_name
    response = requests.get(URL_get_tableid, auth=auth)
    res = json.loads(response.text)
    table_id = res['tables'][0]['id']

    return table_id


def get_tables(wh_id_1: int, schema_1: str, wh_id_2: int, schema_2: str, auth):
    url_1 = "https://app.bigeye.com/api/v1/tables?warehouseId=" + str(wh_id_1) + "&schema=" + schema_1
    url_2 = "https://app.bigeye.com/api/v1/tables?warehouseId=" + str(wh_id_2) + "&schema=" + schema_2

    response_1 = requests.get(url_1, auth=auth)
    res_1 = json.loads(response_1.text)
    response_2 = requests.get(url_2, auth=auth)
    res_2 = json.loads(response_2.text)

    tables_1 = []
    tables_2 = []

    for t in res_1['tables']:
        tables_1.append(t['name'])

    for t in res_2['tables']:
        tables_2.append(t['name'])

    return tables_1, tables_2


def create_tablename_pairs(table1: List[str], table2: List[str]):
    temp_tbl2_matched = []

    for t in table1:
        temp_tbl2_matched.append(process.extract(t, table2, limit=1)[0][0])

    matching_table_pairs = list(zip(table1, temp_tbl2_matched))

    return matching_table_pairs


def get_metrics_for_deltas_table(source_table_id: int, auth):
    url = "https://app.bigeye.com/api/v1/tables/" + str(source_table_id) + "/delta-applicable-metric-types"
    response = requests.get(url, auth=auth)
    res = json.loads(response.text)

    return res


# this is for creating a brand new delta
# source: 1567007
# target: 1567872

def run_a_delta(delta_id: int, auth):
    url = "https://app.bigeye.com/api/v1/metrics/comparisons/tables/run/" + str(delta_id)
    response = requests.get(url, auth=auth)

    if response.status_code == 200:
        return 'Deltas is running'
    else:
        return "Deltas failed to run."


if __name__ == "__main__":

    #bigeye_conf = '/Users/reneeliu/.bigeye/conf/renee_quadpay.conf'
    # api_conf = BasicAuthRequestLibApiConf.load_api_conf(bigeye_conf)
    # client = CoreDatawatchClient(api_conf=api_conf)

    print("I am using GitHub Action to print ... ")
    print("Looks like it's a success -- ")
    rows = 5
    num = rows
    # reverse for loop
    for i in range(rows, 0, -1):
        for j in range(0, i):
            print(num, end=' ')
        print("\r")

    #
    # S_WH_ID = 640
    # S_SCHEMA_NAME = "VAULT.CDP"
    # T_WH_ID = 454
    # T_SCHEMA_NAME = "vault.cdp"
    #
    # source_list = client.get_tables(warehouse_id=[S_WH_ID], schema=[S_SCHEMA_NAME])
    # target_list = client.get_tables(warehouse_id=[T_WH_ID], schema=[T_SCHEMA_NAME])
    #
    # source_names_dict = {}
    # target_names_dict = {}
    #
    # # refactor get_list_tablenames
    # for s in source_list.tables:
    #     source_names_dict[s.name] = s.id
    #
    # for t in target_list.tables:
    #     target_names_dict[t.name] = t.id
    #
    # table_pairs = create_tablename_pairs(source_names_dict.keys(), target_names_dict.keys())
    #
    # for t_pair in table_pairs:
    #     source_t_id = source_names_dict[t_pair[0]]
    #     print('source table id: ' + str(source_t_id))
    #     target_t_id = target_names_dict[t_pair[1]]
    #     print('target table id: ' + str(target_t_id))
    #     x=client.create_delta("Bigeye testing " + S_SCHEMA_NAME + " ." + t_pair[0] + " "
    #                                    + emoji.emojize(':magnifying_glass_tilted_right:') + " " + T_SCHEMA_NAME + " ." + t_pair[1], source_t_id, target_t_id)
    #     delta_id = x.comparison_table_configuration.id
    #     client.run_a_delta(delta_id)