import requests
import base64
import json
import os
import pathlib


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
    headers = {
        'User-Agent': 'Jobsuche/2.9.2 (de.arbeitsagentur.jobboerse; build:1077; iOS 15.1.0) Alamofire/5.4.4',
        'Host': 'rest.arbeitsagentur.de',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded; charset=utf-8',
    }

    data = {
      'client_id': 'c003a37f-024f-462a-b36d-b001be4cd24a',
      'client_secret': '32a39620-32b3-4307-9aa1-511e3d7f48a8',
      'grant_type': 'client_credentials'
    }

    response = requests.post('https://rest.arbeitsagentur.de/oauth/gettoken_cc',
                             headers=headers,
                             data=data,
                             verify=False)

    return response.json()


def search(jwt, what, page):
    """search for jobs. params can be found here: https://jobsuche.api.bund.dev/"""
    params = (
        ('arbeitszeit', 'vz'),
        ('page', str(page)),
        ('size', '100'),
        ('was', what),
    )

    headers = {
        'User-Agent': 'Jobsuche/2.9.2 (de.arbeitsagentur.jobboerse; build:1077; iOS 15.1.0) Alamofire/5.4.4',
        'Host': 'rest.arbeitsagentur.de',
        'OAuthAccessToken': jwt,
        'Connection': 'keep-alive',
    }

    response = requests.get('https://rest.arbeitsagentur.de/jobboerse/jobsuche-service/pc/v4/app/jobs',
                            headers=headers, params=params, verify=False)
    return response.json()


def job_details(jwt, job_ref):

    headers = {
        'User-Agent': 'Jobsuche/2.9.3 (de.arbeitsagentur.jobboerse; build:1078; iOS 15.1.0) Alamofire/5.4.4',
        'Host': 'rest.arbeitsagentur.de',
        'OAuthAccessToken': jwt,
        'Connection': 'keep-alive',
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
