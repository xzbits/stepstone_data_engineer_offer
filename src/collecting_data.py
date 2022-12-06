import requests
import base64
import json
import os
import pathlib
from connect_db import config_parser


def make_subdir(path, dir_name):
    path_dir_name = os.path.join(path, dir_name)
    try:
        os.mkdir(path_dir_name)
    except OSError:
        print("Creation of the temperature with time directory %s failed" % path_dir_name)
    else:
        print("Successfully created the  temperature with time directory %s" % path_dir_name)


def get_jwt():
    """fetch the jwt token object"""
    param_dict = config_parser('oauth.cfg', 'jwt')
    headers = {
        'User-Agent': param_dict['user_agent'],
        'Host': param_dict['host'],
        'Connection': param_dict['connection'],
        'Content-Type': param_dict['content_type'],
    }

    data = {
      'client_id': param_dict['client_id'],
      'client_secret': param_dict['client_secret'],
      'grant_type': param_dict['grant_type']
    }

    response = requests.post('https://rest.arbeitsagentur.de/oauth/gettoken_cc',
                             headers=headers,
                             data=data,
                             verify=False)

    return response.json()


def search(jwt, what, page):
    """search for jobs. params can be found here: https://jobsuche.api.bund.dev/"""
    param_dict = config_parser('oauth.cfg', 'job_offer')
    params = (
        ('arbeitszeit', 'vz'),
        ('page', str(page)),
        ('size', '100'),
        ('was', what),
    )

    headers = {
        'User-Agent': param_dict['user_agent'],
        'Host': param_dict['host'],
        'OAuthAccessToken': jwt,
        'Connection': param_dict['connection'],
    }

    response = requests.get(param_dict['api_url'], headers=headers, params=params, verify=False)
    return response.json()


def job_details(jwt, job_ref):
    param_dict = config_parser('oauth.cfg', 'job_detail')
    headers = {
        'User-Agent': param_dict['user_agent'],
        'Host': param_dict['host'],
        'OAuthAccessToken': jwt,
        'Connection': param_dict['connection'],
    }

    response = requests.get(
        f'https://rest.arbeitsagentur.de/jobboerse/jobsuche-service/pc/v2/jobdetails/{(base64.b64encode(job_ref.encode())).decode("UTF-8")}',
        headers=headers, verify=False)

    return response.json()


def load_data(dir_path, jwt):
    make_subdir(dir_path, "job_offer_data")
    make_subdir(dir_path, "job_detail_data")
    job_offer_path = os.path.join(dir_path, "job_offer_data")
    job_detail_path = os.path.join(dir_path, "job_detail_data")

    for i in range(1, 1000):
        job_offer_data = search(jwt["access_token"], "Data Engineer", i)
        try:
            job_offer_json = job_offer_data['stellenangebote']
        except KeyError:
            break

        # Load job detail records
        job_detail_list = []
        for one_offer in job_offer_json:
            job_detail_list.append(job_details(jwt["access_token"], one_offer['refnr']))

        # Load job offer records
        job_offer_filename = os.path.join(job_offer_path, "job_offer_page_{}.json".format(i))
        with open(job_offer_filename, "w", encoding="utf-8") as file:
            json.dump(job_offer_json, file, ensure_ascii=False, indent=4)

        job_detail_filename = os.path.join(job_detail_path, "job_detail_page_{}.json".format(i))
        with open(job_detail_filename, "w", encoding="utf-8") as file:
            json.dump(job_detail_list, file, ensure_ascii=False, indent=4)


def process_wrangling_data():
    jwt = get_jwt()
    load_data(pathlib.Path().resolve(), jwt)


if __name__ == "__main__":
    process_wrangling_data()
