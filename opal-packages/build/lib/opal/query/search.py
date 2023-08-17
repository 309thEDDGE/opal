import os
import requests
import pandas as pd
import time
import json


class Instance:
    def __init__(
        self,
        api_url=os.environ.get("CATALOG_BACKEND_URL"),
        token=os.environ.get("JUPYTERHUB_API_TOKEN"),
    ):
        self.url = api_url
        self.token = token

        self.query = []
        self.type = None
        self.id = None

    def _with(self, key, value):
        self.__reset()
        self.query.append(dict(key=key, value=value))
        return self

    def _type(self, type):
        self.type = type
        return self

    def _and(self, key, value):
        self.query.append(dict(key=key, value=value, operator="AND"))
        return self

    def _or(self, key, value):
        self.query.append(dict(key=key, value=value, operator="OR"))
        return self

    def _not(self, key, value):
        self.query.append(dict(key=key, value=value, operator="NOT"))
        return self

    def __hexify(self, d):
        d = json.dumps(d)
        d = d.encode("utf-8")
        d = d.hex()
        return d

    def __reset(self):
        self.query = []
        self.type = None

    def one(self, id):
        self.id = id
        r = self.search()
        self.id = None
        return r

    def search(self):
        query_str = self.url + "/instance"
        query_parts = []

        if len(self.query) > 0:
            hex_querystring = self.__hexify(self.query)
            query_parts.append(f"search={hex_querystring}")

        if self.type is not None:
            query_parts.append(f"kind_type={self.type}")

        if self.id is not None:
            query_parts.append(f"kind_id={self.id}")

        joined_parts = "&".join(query_parts) if len(query_parts) > 0 else None

        if joined_parts is not None:
            query_str += f"?{joined_parts}"

        r = requests.get(query_str,
                         headers={"Authorization": "token %s" % self.token})

        json_response = r.json()
        return Results(json_response)


class Results:
    def __init__(self, response_dict):
        self.results = response_dict.get("data")

    def all(self):
        return self.results

    def ids(self):
        return [i.get("kind_id") for i in self.results]

    def filter(self, lx):
        return [i for i in self.results if lx(i) is True]

    def reduce(self, lx):
        return [lx(i) for i in self.results]
