import time

import requests
import json
import os
import sys
from dateutil import rrule
import datetime
import config
import csv
sys.path.append("..")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class TenantData():

    def all_dates(self):
        all_data = []
        current_year = datetime.date.today().year
        for year in range(2023, current_year + 1):
            for month in range(1, 13):
                start_date = datetime.date(year, month, 1)
                if month == 12:
                    next_month_start = datetime.date(year + 1, 1, 1)
                else:
                    next_month_start = datetime.date(year, month + 1, 1)
                all_data.append(
                    {'start': start_date.strftime('%Y-%m-%d'),
                     'next_month_start': next_month_start.strftime('%Y-%m-%d')})

        return all_data

    def meters_data(self, refreshToken):
        token = self.get_refresh_token(refreshToken)
        url = config.base_url

        query = '''
        query MetersPage($tenancyId: Int!, $date: String) {  meterData(tenancyId: $tenancyId, date: $date) {    metersData {      id      kind      number      value      timestamp      unit      room      __typename    }    __typename  }  tenancy: tenancyById(id: $tenancyId) {    id    apartment {      apartment      address      __typename    }    __typename  }}
        '''
        variables = {"tenancyId": config.tenancyId, "date": f"{datetime.date.today()}T21:20:17.883Z"}
        headers = {
            'Authorization': f'JWT {token}',
            'Content-Type': 'application/json',
            'Cookie': config.cookie
        }

        response = requests.request("POST", url, headers=headers, json={'query': query, 'variables': variables})

        resp = json.loads(response.text)
        return resp['data']['meterData']['metersData']

    def get_refresh_token(self, refreshToken):
        url = config.base_url
        query = '''
        mutation refreshToken($refreshToken: String!) {          refreshToken(refreshToken: $refreshToken) {             payload             token              refreshExpiresIn          }        }
        '''
        variables = {"refreshToken": f"{refreshToken}"}
        headers = {
            'Content-Type': 'application/json',
            'Cookie': config.cookie
        }

        response = requests.request("POST", url, headers=headers, json={'query': query, 'variables': variables})
        resp = json.loads(response.text)
        return resp['data']['refreshToken']['token']

    def write_data_in_csv(self, refreshToken):
        dates = self.all_dates()
        all_meters_data = self.meters_data(refreshToken)
        for meter in all_meters_data:
            print(f"Scraping data for meter {meter['number']}")
            for year in reversed(dates):
                print(year)
                for j in range(0, 4):
                    try:
                        token = self.get_refresh_token(refreshToken)
                        url = config.base_url
                        query = '''
                        query ConsumptionGraphData(                    $tenancyId: Int!                    $spanType: String!                    $kind: String!                    $room: String                    $meterId: Int                    $compareWith: String!                    $periodBegin: String                    $periodEnd: String                  ) {                    consumptionByKind(                      tenancyId: $tenancyId                      spanType: $spanType                      kind: $kind                      room: $room                      meterId: $meterId                      periodBegin: $periodBegin                      periodEnd: $periodEnd                      compareWith: $compareWith                    ) {                      consumptionGraphData {                        value                        valueCompare                        timestamp                        label                      }                    }                  }
                        '''
                        variables = {"kind": meter['kind'], "tenancyId": config.tenancyId, "meterId": meter['id'],
                                     "periodBegin": f"{year['start']}T19:00:00.000Z",
                                     "periodEnd": f"{year['next_month_start']}T18:59:59.999Z", "spanType": "day",
                                     "compareWith": "property"}
                        headers = {
                            'Authorization': f'JWT {token}',
                            'Content-Type': 'application/json',
                            'Cookie': config.cookie
                        }

                        response = requests.request("POST", url, headers=headers,
                                                    json={'query': query, 'variables': variables})
                        break
                    except Exception as e:
                        print(f"--------Graphql request fail {j} time---------")
                        print(e)
                        time.sleep(5)

                resp = json.loads(response.text)
                if len(resp['data']['consumptionByKind']['consumptionGraphData']) == 0:
                    continue
                for i in resp['data']['consumptionByKind']['consumptionGraphData']:
                    if i['value'] is None or i['valueCompare'] is None:
                        continue

                    data = [{
                        'login_id': config.username, 'meter_type': meter['kind'],
                        'meter_number': meter['number'],
                        'updated_at': str(datetime.date.today()),
                        f'Meter {meter["number"]}': {'data': {f'{year["start"].split("-")[0]}': {
                            f'{i["timestamp"]}': {
                                'value': i['value'],
                                'valueCompare': i['valueCompare'],
                                'unit': meter['unit']}}},
                            'address': config.address,
                            'period': 'daily'}
                    }]

                    with open(config.csv_file_name, 'w', newline='') as csvfile:
                        fieldnames = data[0].keys()
                        csv_writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                        if not os.path.exists(config.csv_file_name):
                            csv_writer.writeheader()
                        csv_writer.writerows(data)

    def get_token(self):
        url = config.base_url

        payload = "{\"query\":\"mutation tokenAuth($email: String!, $password: String!) {    tokenAuth(email: $email, password: $password) {      payload      refreshExpiresIn      token      refreshToken    }  }\",\"variables\":{\"email\":\"rene.hansen@fk.dk\",\"password\":\"Omdeler01\"}}"
        headers = {
            'Host': config.host,
            'Origin': config.origin,
            'Referer': config.referer,
            'Content-Type': 'application/json',
            'Cookie': config.cookie
        }

        response = requests.request("POST", url, headers=headers, data=payload)
        resp = json.loads(response.text)
        token = resp['data']['tokenAuth']['token']
        refreshToken = resp['data']['tokenAuth']['refreshToken']
        return refreshToken, token

    def scrape(self):
        refreshToken, token = self.get_token()
        self.write_data_in_csv(refreshToken)


if __name__ == "__main__":
    d = TenantData()
    d.scrape()
