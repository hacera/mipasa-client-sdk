# -*- coding: utf-8 -*-
# Copyright (C) 2020, HACERA, INC. All Rights Reserved.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE X CONSORTIUM BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
# Except as contained in this notice, the name of MiPasa or HACERA shall not be used in advertising or otherwise to promote the sale, use or other dealings in this Software without prior written authorization from HACERA.
#
# HACERA is a trademark of HACERA, INC.

from urllib.request import Request, urlopen
from urllib.parse import urlencode
import json
import datetime
import time
from io import StringIO
import csv
import base64

PROD_API = 'https://unbounded.network/api/mipasa'

_DEFAULT_URL = PROD_API


def _guess_mime_type(bytes):
    try:
        import magic
        return magic.from_buffer(bytes, mime=True)
    except ImportError:
        raise ClientError('Python-Magic is required to detect MIME type automatically')


class Client(object):
    """MiPasa REST API Client

    This is the main entrypoint for interaction with the API.

    Example use:

    .. highlight:: python
    .. code-block:: python

        # Retrieve the list of remote data feeds

        import mipasa

        client = mipasa.Client('8afde...')
        all_feeds = client.list_feeds()
    """

    def __init__(self, api_key=None, api_url=None):
        """Creates a new Client instance"""
        self.api_key = api_key
        self.api_url = api_url if api_url is not None else _DEFAULT_URL

    def _http_request(self, method, url, data=None, send_json=True, get_json=True, headers={}):
        if 'X-UN-API-Key' not in headers and self.api_key is not None:
            headers['X-UN-API-Key'] = self.api_key
        if data is not None:
            if send_json:
                data = json.dumps(data).encode('utf-8')
            else:
                data = urlencode(data).encode('utf-8')
        req = Request(url, method=method, data=data)
        if get_json:
            headers['Accept'] = 'application/json'
        for h in headers:
            req.add_header(h, headers[h])
        try:
            r = urlopen(req)
            content = r.read()
            code = r.getcode()
        except Exception as e:
            noraise = False
            try:
                content = e.read()
                code = e.getcode()
                noraise = True
            except:
                pass
            if not noraise:
                raise e
        if get_json:
            return json.loads(content.decode('utf-8')), code
        return content, code

    @staticmethod
    def convert_date_to_api(dt):
        if dt is None:
            return None
        return int(time.mktime(dt.timetuple()) * 1000 + dt.microsecond / 1000)

    @staticmethod
    def convert_date_from_api(timestamp):
        if timestamp is None:
            return None
        return datetime.datetime.fromtimestamp(float(timestamp) / 1000)

    def list_feeds(self):
        """Retrieves a list of all data feeds from the server

        :returns: A list of all data feeds
        :rtype: list of DataFeed
        :raises: ApiError, if backend failed to return the data
        """
        feeds_json, _ = self._http_request('GET', '%s/feeds?include=feed:files' % self.api_url)
        if 'error' in feeds_json:
            raise ApiError(feeds_json['error'])
        return [DataFeed.from_json(self, x) for x in feeds_json]

    def get_feed_by_id(self, id):
        """Retrieves a specific data feed by unique ID

        :returns: A single data feed
        :rtype: DataFeed
        :raises: ApiError, if backend failed to return the data or it doesn't exist
        """
        feed_json, code = self._http_request('GET', '%s/feeds/%s' % (self.api_url, id))
        if 'error' in feed_json:

            raise ApiError(feed_json['error'])
        return DataFeed.from_json(self, feed_json)

    def get_feed_by_name(self, name):
        """Retrieves a specific data feed by name

        :returns: A single data feed
        :rtype: DataFeed
        :raises: ApiError, if backend failed to return the data or it doesn't exist
        """
        feeds = self.list_feeds()
        feed = [x for x in feeds if x.name == name]
        if not feed:
            return None
        return feed[0]

    def get_current_user(self):
        """Retrieves the current user

        :returns: Current user (by API Key)
        :rtype: VisualUser
        :raises: ApiError, if backend failed to return the data
        """
        user_json, _ = self._http_request('GET', '%s/currentUser' % self.api_url)
        if 'error' in user_json:
            raise ApiError(user_json['error'])
        return VisualUser.from_json(user_json)


class DataFeed(object):
    """Data feed object, either created locally or received from the server

    Example use:

    .. highlight:: python
    .. code-block:: python

        # Retrieve some information about a remote data feed

        feed = client.get_feed_by_name('JHU CSSE')
        print('Feed is created by %s on %s'%(feed.get_user().first_name, repr(feed.created_at)))
    """
    def __init__(self, client):
        self.client = client
        self.last_id = None
        self.id = None
        """ID of this data feed"""
        self.name = None
        """Name of this data feed; free form, human-readable"""
        self.user_id = None
        """User ID of the creator of this feed"""
        self.created_at = datetime.datetime.now()
        """datetime.datetime: Creation date"""
        self.approved_at = None
        """datetime.datetime: Approval date (or None)"""
        self.approved_by = None
        """User ID of the admin that approved this feed"""
        self.icon = None
        """ApiImage or str: Icon of this data feed (or None)"""
        self.files = None
        """list of File: Files in this feed"""
        self.short_description = None
        """Short description of this data feed"""
        self.full_description = None
        """Full description of this data feed"""
        self.source = None
        """Source of data for this data feed (may not be the same as the uploader)"""
        self.tags = None
        """list of str: Tags for this data feed"""
        self.categories = None
        """list of str: Categories for this data feed"""
        self.license = None
        """License"""
        self.license_updated_at = None
        """Last date of License being updated"""
        self.user = None
        self.approved_by_user = None

    def _load_from_json(self, obj):
        self.last_id = self.id = obj['id']
        self.name = obj['name']
        self.user_id = obj['userId']
        self.created_at = Client.convert_date_from_api(obj['createdAt'])
        self.approved_at = Client.convert_date_from_api(obj['approvedAt'])
        self.user = obj['user']
        self.approved_by_user = obj['approvedByUser']
        self.approved_by = obj['approvedBy']
        self.icon = obj['icon']
        self.files = [File.from_json(self, x) for x in obj['files']]
        self.short_description = obj['shortDesc']
        self.full_description = obj['fullDesc']
        self.source = obj['source']
        self.tags = obj['tags']
        self.categories = obj['categories']
        self.license = obj['license']
        self.license_updated_at = Client.convert_date_from_api(obj['licenseUpdatedAt'])

    @classmethod
    def from_json(cls, client, obj):
        """Creates a new instance from server response.

        :returns: A new DataFeed object
        :rtype: DataFeed
        """
        o = cls(client)
        o._load_from_json(obj)
        return o

    def get_user(self):
        """Returns additional user info about the creator of this feed

        :returns: User info
        :rtype: VisualUser
        """

    def get_approved_by(self):
        """Returns additional user info about the admin that approved this feed

        :returns: User info
        :rtype: VisualUser
        """
        return VisualUser.from_json(self.approved_by_user) if self.approved_by_user is not None else None

    def to_json(self):
        """Converts an instance of `DataFeed` to a format compatible with MiPasa REST API

        :returns: A JSON object
        :rtype: dict
        """
        o = {
            'id': self.id,
            'name': self.name,
            'userId': self.user_id,
            'createdAt': Client.convert_date_to_api(self.created_at),
            'approvedAt': Client.convert_date_to_api(self.approved_at),
            'approvedBy': self.approved_by,
            'icon': self.icon.to_base64() if type(self.icon) == ApiImage else self.icon,
            'files': [x.to_json() for x in self.files] if self.files is not None else None,
            'shortDesc': self.short_description,
            'fullDesc': self.full_description,
            'source': self.source,
            'tags': self.tags,
            'categories': self.categories,
            'license': self.license,
            'licenseUpdatedAt': Client.convert_date_to_api(self.license_updated_at)
        }
        o_keys = [k for k in o]
        for k in o_keys:
            if o[k] is None:
                del o[k]
        return o

    def __repr__(self):
        return repr(self.to_json())

    def is_approved(self):
        """Returns `True` if this data feed is approved

        :returns: Whether this data feed is approved
        :rtype: bool
        """
        return self.approved_at is not None

    def update(self):
        """Loads the latest version of this data feed from the server

        :raises: ClientError, if data is only local
        :raises: ApiError, if backend failed to return the data
        """
        if self.last_id is not None:
            response, _ = self.client._http_request('GET', '%s/feeds/%s'%(self.client.api_url, self.last_id))
            if 'error' in response:
                raise ApiError(response['error'])
            self._load_from_json(response)
        else:
            raise ClientError('Feed is not known remotely')

    def save(self):
        """Creates or updates this data feed on the server.

        :raises: ApiError, if backend failed to store the data
        """
        if self.last_id is not None:
            response, _ = self.client._http_request('PUT', '%s/feeds/%s'%(self.client.api_url, self.last_id), self.to_json())
        else:
            response, _ = self.client._http_request('POST', '%s/feeds'%(self.client.api_url), self.to_json())
        if 'error' in response:
            raise ApiError(response['error'])
        self._load_from_json(response)

    def list_versions(self):
        """Lists all available versions for this data feed.

        :returns: A list of all data versions
        :rtype: list of DataVersion
        :raises: ClientError, if data is only local
        :raises: ApiError, if backend failed to return the list
        """
        if self.last_id is None:
            raise ClientError('Feed is not known remotely')
        versions_json, _ = self.client._http_request('GET', '%s/data?feedId=%s'%(self.client.api_url, self.last_id))
        if 'error' in versions_json:
            raise ApiError(versions_json['error'])
        return [DataVersion.from_json(self, x) for x in versions_json]

    def get_latest_version(self):
        """Returns the latest revision of data in this feed.

        :returns: The latest data version
        :rtype: DataVersion
        :raises: ClientError, if data is only local
        :raises: ApiError, if backend failed to return the list
        """
        if self.last_id is None:
            raise ClientError('Feed is not known remotely')
        versions_json, _ = self.client._http_request('GET', '%s/data?feedId=%s&latest'%(self.client.api_url, self.last_id))
        if 'error' in versions_json:
            raise ApiError(versions_json['error'])
        if not versions_json:
            return None
        return DataVersion.from_json(self, versions_json[0])

    def get_file(self, filename, ignore_case=False):
        """Returns the file object with the specified filename.

        :returns: Data feed file by name
        :rtype: File or None
        """
        file = [x for x in self.files if x.name == filename or ignore_case and x.name.lower() == filename.lower()]
        if not file:
            return None
        return file[0]

    def new_version(self):
        """Creates a new DataVersion object linked to this feed.

        :returns: A new DataVersion object
        :rtype: DataVersion
        """
        version = DataVersion(self)
        version.feed_id = self.id
        return version


class DataVersion(object):
    """Data revision object, either created locally or received from the server

    Example use:

    .. highlight:: python
    .. code-block:: python

        # Store a normalized table as CSV locally

        import csv

        feed = client.get_feed_by_name('ECDC')
        version = feed.get_latest_version()
        ECDC_Cases = feed_latest_version.get_transformed('ECDC_Cases')

        with codecs.open('ECDC_Cases.csv', 'w', encoding='utf-8') as f:
            writer = csv.writer(f)
            for row in ECDC_Cases:
                writer.writerow(row)

    """
    def __init__(self, feed):
        self.client = feed.client
        self.last_id = None
        self.id = None
        self.feed = feed
        """ID of this data revision"""
        self.feed_id = feed.id
        """ID of the feed that this revision belongs to"""
        self.created_at = None
        """datetime.datetime: Creation date"""
        self.transform_id = None
        """ID of transformation code (or None)"""
        self.transform_result = None
        """Transformation result (or None)"""
        self.transform_status = None
        """Transformation status (or None)"""
        self.transformed_at = None
        """datetime.datetime: Transformation date (or None)"""
        self.transformed_tables = None
        """List of normalized tables emitted after transformation"""
        self.files = []
        """list of File: Files in this feed"""

    def _load_from_json(self, obj):
        self.last_id = self.id = obj['id']
        self.feed_id = obj['feedId']
        self.created_at = Client.convert_date_from_api(obj['createdAt'])
        self.transform_id = obj['transformId']
        self.transform_result = obj['transformResult']
        self.transform_status = obj['transformStatus']
        self.transformed_at = Client.convert_date_from_api(obj['transformedAt'])
        self.transformed_tables = obj['transformedTables']
        self.files = [File.from_json(self, x) for x in obj['files']] if obj['files'] is not None else []

    @classmethod
    def from_json(cls, client, obj):
        """Creates a new instance from server response.

        :returns: A new DataVersion object
        :rtype: DataVersion
        """
        o = cls(client)
        o._load_from_json(obj)
        return o

    def to_json(self):
        """Converts an instance of `DataVersion` to a format compatible with MiPasa REST API

        :returns: A JSON object
        :rtype: dict
        """
        o = {
            'id': self.id,
            'feedId': self.feed_id,
            'createdAt': Client.convert_date_to_api(self.created_at),
            'transformId': self.transform_id,
            'transformResult': self.transform_result,
            'transformStatus': self.transform_status,
            'transformedAt': Client.convert_date_to_api(self.transformed_at),
            'transformedTables': self.transformed_tables,
            'files': [x.to_json() for x in self.files] if self.files is not None else []
        }
        o_keys = [k for k in o]
        for k in o_keys:
            if o[k] is None:
                del o[k]
        return o

    def __repr__(self):
        return repr(self.to_json())

    def is_transformed(self):
        """Returns `True` if this data version was transformed successfully

        :returns: Whether transformation happened and succeeded
        :rtype: bool
        """
        return self.transform_id is not None and self.transform_status == 'succeeded'

    def is_transforming(self):
        """Returns `True` if this data version is currently being transformed

        :returns: Whether transformation is currently in progress
        :rtype: bool
        """
        return self.transform_status != 'pending' and self.transform_status != 'failed' and self.transform_id is not None

    def update(self, include_files=False):
        """Loads the latest information about this data version from the server (including files, if requested)

        :raises: ClientError, if data is only local
        :raises: ApiError, if backend failed to return the data
        """
        if self.last_id is not None:
            response, _ = self.client._http_request('GET', '%s/data/%s%s'%(self.client.api_url, self.last_id, '?include=data:files' if include_files else ''))
            if 'error' in response:
                raise ApiError(response['error'])
            self._load_from_json(response)
        else:
            raise ClientError('Data version is not known remotely')

    def save(self):
        """Creates this data version on the server if it does not exist.

        :raises: ClientError, if this is an existing object
        :raises: ApiError, if backend failed to store the data
        """
        if self.last_id is not None:
            raise ClientError('Data modification is not allowed')
        response, _ = self.client._http_request('POST', '%s/data'%self.client.api_url, self.to_json())
        if 'error' in response:
            raise ApiError(response['error'])
        self._load_from_json(response)

    def get_transformed(self, table):
        """Retrieves normalized table. Requires transform to be applied to this data version.

        .. deprecated:: 1.0
           Use DataFeed.get_file() with output files instead.

        :returns: Specified table as CSV (list of lists of strings, including header)
        :rtype: list
        :raises: ClientError, if data is not transformed or was not generated for this version
        """
        self.update()
        if not self.is_transformed():
            raise ClientError('Data is not transformed')
        if table not in self.transformed_tables:
            raise ClientError('Table \'%s\' it not known'%table)
        response, _ = self.client._http_request('GET', '%s/datasets/%s.csv?dataId=%s'%(self.client.api_url, table, self.last_id), get_json=False)
        # check if this is not a CSV
        try:
            e = json.loads(response)
            if 'error' in e:
                raise ApiError(e['error'])
        except:
            pass
        # parse csv
        f = StringIO(response.decode('utf-8'))
        reader = csv.reader(f)
        return [x for x in reader]


class VisualUser(object):
    """User details provided with some objects (such as name or avatar)"""

    def __init__(self):
        self.id = None
        self.first_name = None
        self.last_name = None
        self.avatar = None

    def _load_from_json(self, j):
        self.id = j['id']
        self.first_name = j['name']
        self.last_name = j['lastName']
        self.avatar = j['avatar'] or None

    @classmethod
    def from_json(cls, j):
        """Creates a new instance from server response.

        :returns: A new VisualUser object
        :rtype: VisualUser
        """
        obj = VisualUser()
        obj._load_from_json(j)
        return obj

    def to_json(self):
        return {
            'id': self.id,
            'name': self.first_name,
            'lastName': self.last_name,
            'avatar': self.avatar
        }


class ApiImage(object):
    """Image that is uploaded to the server"""
    def __init__(self):
        self.mime_type = None
        self.bytes = None
        self.remote_filename = None

    @classmethod
    def from_file(cls, filename, mime_type=None):
        out = ApiImage()
        with open(filename, 'rb') as f:
            out.bytes = f.read()
        out.mime_type = mime_type if mime_type is not None else _guess_mime_type(out.bytes)
        return out

    @classmethod
    def from_bytes(cls, bytes, mime_type=None):
        out = ApiImage()
        out.bytes = bytes
        out.mime_type = mime_type if mime_type is not None else _guess_mime_type(out.bytes)
        return out

    def to_base64(self):
        if self.bytes is None or self.mime_type is None:
            return None
        return 'data:%s;base64,%s'%(self.mime_type,base64.b64encode(self.bytes).decode('ascii'))


class File(object):
    """Specific versioned file in a data feed"""
    def __init__(self, feed_or_data):
        if issubclass(type(feed_or_data), DataFeed):
            feed = feed_or_data
            self.client = feed.client
            self.feed = feed
            self.data = None
        elif issubclass(type(feed_or_data), DataVersion):
            data = feed_or_data
            self.client = data.client
            self.data = data
            self.feed = data.feed
        else:
            raise ClientError('Argument must be of type DataFeed or DataVersion')
        self.id = None
        """ID of this file"""
        self.last_id = None
        self.name = None
        """Name of this file"""
        self.data_id = None if self.data is None else self.data.id
        """Version that this file belongs to"""
        self.mime_type = None
        """str: MIME type"""
        self.base64 = False
        """bool: Whether this file is base64-encoded"""
        self.size = 0
        """int: Size of this file in bytes (not always equal to size of payload, see base64)"""
        self.payload = None
        """str: Payload, base64-encoded if binary. This field is only used during file upload"""
        self.role = None
        """str: File role — either 'input' or 'output'.
        Input files are uploaded by client, output files are produced by the server"""
        self.downloads = 0
        """int: Count of file downloads for this version"""
        self.total_downloads = 0
        """int: Count of file downloads for all versions"""

    def _load_from_json(self, j):
        self.last_id = self.id = j['id']
        self.name = j['name']
        self.data_id = j['dataId']
        self.mime_type = j['mimeType']
        self.base64 = j['base64']
        self.size = j['size']
        self.payload = j['payload']
        self.role = j['role']
        self.downloads = j['downloads']
        self.total_downloads = j['totalDownloads']

    @classmethod
    def from_json(cls, version, j):
        """Creates a new instance from server response.

        :returns: A new File object
        :rtype: File
        """
        o = File(version)
        o._load_from_json(j)
        return o

    @classmethod
    def from_bytes(self, version, bytes, mime_type=None):
        """Creates a new instance from local (in-memory) file.

        :returns: a new File object
        :rtype: File
        """
        if type(bytes) == str:
            bytes = bytes.encode('utf-8')
        o = File(version)
        if mime_type is None:
            mime_type = _guess_mime_type(bytes)
        if mime_type.index('text/') == 0 or mime_type == 'application/json' or mime_type == 'application/x-yaml':
            o.base64 = False
            o.size = len(bytes)
            o.payload = bytes.decode('utf-8')
        else:
            o.base64 = True
            o.size = len(bytes)
            o.payload = base64.b64encode(bytes).decode('utf-8')
        o.mime_type = mime_type
        o.role = 'input'
        return o

    def to_json(self):
        """Converts an instance of `File` to a format compatible with MiPasa REST API

        :returns: A JSON object
        :rtype: dict
        """
        o = {
            'id': self.id,
            'name': self.name,
            'dataId': self.data_id,
            'mimeType': self.mime_type,
            'base64': self.base64,
            'size': self.size,
            'payload': self.payload,
            'role': self.role
        }
        o_keys = [k for k in o]
        for k in o_keys:
            if o[k] is None:
                del o[k]
        return o

    def __repr__(self):
        return repr(self.to_json())

    def get(self, at_version=None):
        """Retrieves file payload for the specified version (or default/latest known, if not available)

        :returns: Specified file as str or bytes (depending on MIME type)
        :rtype: str or bytes
        :raises: ClientError, if data is not yet uploaded
        :raises: ApiError, if server failed to return the data or file is not found at the specified version
        """
        if self.last_id is None:
            raise ClientError('File is not uploaded yet')
        if self.feed.last_id is None:
            raise ClientError('Feed is not uploaded yet')
        if at_version is None:
            at_version = self.data.last_id if self.data is not None else self.data_id
        if at_version is None:
            raise ClientError('Data version is not known')
        response, code = self.client._http_request('GET', '%s/download/%s/%s?atVersion=%s' % (self.client.api_url, self.feed.last_id, self.name, at_version), get_json=False)
        # check if this is not a CSV
        try:
            e = json.loads(response)
            if 'error' in e and code != 200:
                raise ApiError(e['error'])
            elif code != 200:
                raise ApiError('Unknown API error (code = %d)'%code)
        except:
            pass
        if not self.base64:
            response = response.decode('utf-8')
        return response

    def get_as_csv(self, at_version=None):
        """Same as get(), but automatically converts data to CSV. Convenience method.

        :returns: Specified file as CSV
        :rtype: list
        :raises: ClientError, if MIME type is not plaintext or CSV
        :raises: ApiError, if server failed to return the data or file is not found at the specified version
        """
        if self.mime_type != 'text/plain' and self.mime_type != 'text/csv':
            raise ClientError('Invalid MIME type: %s != text/csv'%self.mime_type)
        f = self.get(at_version=at_version)
        # parse csv
        f = StringIO(f)
        reader = csv.reader(f)
        return [x for x in reader]

    def get_as_json(self, at_version=None):
        """Same as get(), but automatically converts data to JSON. Convenience method.

        :returns: Specified file as JSON
        :raises: ClientError, if MIME type is not plaintext or JSON
        :raises: ApiError, if server failed to return the data or file is not found at the specified version
        """
        if self.mime_type != 'text/plain' and self.mime_type != 'application/json':
            raise ClientError('Invalid MIME type: %s != application/json'%self.mime_type)
        f = self.get(at_version=at_version)
        # parse json
        return json.loads(f)


class ClientError(Exception):
    """Error that is originating in the client — for example, clientside validation error"""
    pass


class ApiError(Exception):
    """Error that is originating on the server"""
    pass