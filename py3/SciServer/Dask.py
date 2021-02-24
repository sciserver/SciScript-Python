import requests
from requests.exceptions import HTTPError
from base64 import b64decode
from pathlib import Path
from dask.distributed import Client
from distributed.security import Security
from os.path import expanduser
import SciServer.Authentication
import SciServer.Config
import json

def getClient(ref_id=None):
    """
    Creates a new client for the specified Dask cluster.
    
    If `ref_id` is not set, cluster connection
    properties will be read from the ``~/dask-cluster.json`` file
    injected into new SciServer Compute containers automatically
    when they are created with an attached Dask cluster.
    
    :param ref_id: Dask cluster reference ID in SciServer Compute,
        defaults to `None`
    
    :return: Dask client object
    :rtype: :class:`dask.distributed.Client`
    """
    token = SciServer.Authentication.getToken()
    data = None
    if (ref_id is None):
        with open(expanduser('~/dask-cluster.json')) as f:
            data = json.load(f)
    else:
        try:
            response = requests.get(''.join([SciServer.Config.ComputeUrl.rstrip('/'), '/api/dask/clusters/', ref_id]),
                                    params = {'connectionInfo': 'true'},
                                    headers = {'X-Auth-Token': token})
            response.raise_for_status()
            data = response.json()['connection']
        except HTTPError as err:
            try:
                msg = str(err) + '\n' + response.json()['error']['stackTrace']
            except:
                raise err
            raise HTTPError(msg)
    
    base_dir = expanduser('~/.sciserver/dask')
    Path(base_dir).mkdir(parents=True, exist_ok=True)
    
    with open(base_dir + '/ca.pem', 'w+b') as f:
        f.write(b64decode(data['ca']))
    
    with open(base_dir + '/client-cert.pem', 'w+b') as f:
        f.write(b64decode(data['clientCert']))
    
    with open(base_dir + '/client-key.pem', 'w+b') as f:
        f.write(b64decode(data['clientKey']))
    
    scheduler_url = data['schedulerUrl']
    client = Client(
        scheduler_url, 
        security = Security(tls_ca_file = base_dir + '/ca.pem',
                            tls_client_cert = base_dir + '/client-cert.pem',
                            tls_client_key = base_dir + '/client-key.pem',
                            require_encryption=True))
    
    return client
