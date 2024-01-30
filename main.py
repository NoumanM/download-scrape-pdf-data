import requests
import json
import os
import sys
import re
import config
from PyPDF2 import PdfReader
import csv
import datetime

sys.path.append("..")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class DownloadAndScrapePDF():
    def __init__(self):
        self.base_url = config.base_url

    def write_data_into_db(self, token):
        query = "query AllocationStatements($tenancyId: Int!) {  tenancy: tenancyById(id: $tenancyId) {    id    apartment {      address      apartment      __typename    }    allocationStatements {      fileName      url      periodStartDate      periodEndDate      expenseTypes      __typename    }    __typename  }}"
        variables = {"tenancyId": config.tenancyId}
        headers = {
            'Authorization': f'JWT {token}',
            'Content-Type': 'application/json',
            'Cookie': config.cookie
        }

        response = requests.request("POST", self.base_url, headers=headers,
                                    json={"query": query, "variables": variables}, timeout=60)

        resp = json.loads(response.text)
        for i in resp['data']['tenancy']['allocationStatements']:
            book_response = requests.get(url=i['url'])
            if not os.path.isdir("techem_pdfs"):
                os.mkdir("techem_pdfs")
            if os.path.exists(f"{BASE_DIR}/techem_pdfs/invoice.pdf"):
                os.remove(f"{BASE_DIR}/techem_pdfs/invoice.pdf")
            with open(f'{BASE_DIR}/techem_pdfs/invoice.pdf', 'wb') as file:
                file.write(book_response.content)
            with open(f"{BASE_DIR}/techem_pdfs/invoice.pdf", "rb") as pdf_file:
                reader = PdfReader(pdf_file)
                page_2 = reader.pages[1].extract_text()
                page_1 = reader.pages[0].extract_text()
                match = re.search(r'Naturgas\s+(\d[\d,.]*,?\d*)\s+\d{2}\.\d{2}\.\d{4}', page_2)
                if not match:
                    match = re.search(r'Naturgas\s+(\d[\d,.]*,?\d*)\s+\d{2}\.\d{2}\.\d{4}', page_1)
                if match:
                    value = match.group(1).replace(".", '')
                    value = value.replace(',', '.')
                    print(f"Scraped value: {value}")
                    data = [{
                        'login_id': config.username, 'updated_at': str(datetime.date.today()),
                        "data": [{"value": value, "period": {"from": i['periodStartDate'], "to": i['periodEndDate']}}],
                        "address": "Ryttermarken 4B, 5700 Svendborg", "period": "yearly"
                    }]

                    # Open the CSV file in write mode
                    with open(config.csv_file_name, 'w', newline='') as csvfile:
                        fieldnames = data[0].keys()
                        csv_writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                        # Write the header row
                        csv_writer.writeheader()
                        csv_writer.writerows(data)

    def get_token(self):
        query = "mutation tokenAuth($email: String!, $password: String!) {    tokenAuth(email: $email, password: $password) {      payload      refreshExpiresIn      token      refreshToken    }  }"
        variables = {"email": config.username, "password": config.password}
        headers = {
            'Content-Type': 'application/json',
            'Cookie': 'csrftoken=Dbv4xbdkirOgd4qLmr3NKINcT5tQhluACnFwfKZVEtboSa3jzb2AimZjlrW0slzA'
        }

        response = requests.request("POST", self.base_url, headers=headers,
                                    json={"query": query, "variables": variables}, timeout=60)
        resp = json.loads(response.text)
        token = resp['data']['tokenAuth']['token']
        refresh_token = resp['data']['tokenAuth']['refreshToken']
        return refresh_token, token

    def scrape(self):
        refresh_token, token = self.get_token()
        self.write_data_into_db(token)


if __name__ == "__main__":
    d = DownloadAndScrapePDF()
    d.scrape()
