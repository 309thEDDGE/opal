import os

import intake
from intake.auth.base import BaseClientAuth
from intake.catalog.remote import RemoteCatalog


class JupyterHubClientAuth(BaseClientAuth):
    def __init__(self):
        self.api_token = os.environ.get("JUPYTERHUB_API_TOKEN")

    def get_headers(self):
        return {"Authorization": self.api_token}


class HackedRemote(RemoteCatalog):
    """
    This is only necessary because of a bug in intake that was resolve in
    this PR: https://github.com/intake/intake/pull/618#event-5435151687

    Once the next version of intake after 0.6.3 is released we can remove this.
    """

    def __init__(self, *args, **kwargs):

        super(HackedRemote, self).__init__(*args, **kwargs)

        self.pmode = "never"


def open_catalog(uri=None, auth=JupyterHubClientAuth()):
    cat = HackedRemote(
        uri, auth=auth, persist_mode="never", ttl=1000000000000, page_size=50
    )
    return cat
