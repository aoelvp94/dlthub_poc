from itertools import islice

import dlt
from typing import TYPE_CHECKING, Any, Dict, Iterable, Iterator, List, Optional, Tuple

from dlt.common.configuration.specs import BaseConfiguration, configspec
from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.collection import Collection
from pymongo.cursor import Cursor

from dlt.sources import DltResource
from dlt.common.typing import TDataItem

if TYPE_CHECKING:
    TMongoClient = MongoClient[Any]
    TCollection = Collection[Any]  # type: ignore
    TCursor = Cursor[Any]
else:
    TMongoClient = Any
    TCollection = Any
    TCursor = Any


CHUNK_SIZE = 10000

class CollectionLoader:
    def __init__(
        self,
        client: TMongoClient,
        collection: TCollection,
        incremental: Optional[dlt.sources.incremental[Any]] = None,
    ) -> None:
        self.client = client
        self.collection = collection
        self.incremental = incremental
        if incremental:
            self.cursor_field = incremental.cursor_path
            self.last_value = incremental.last_value
        else:
            self.cursor_column = None
            self.last_value = None

    @property
    def _filter_op(self) -> Dict[str, Any]:
        if not self.incremental or not self.last_value:
            return {}
        if self.incremental.last_value_func is max:
            return {self.cursor_field: {"$gte": self.last_value}}
        elif self.incremental.last_value_func is min:
            return {self.cursor_field: {"$lt": self.last_value}}
        return {}

    def load_documents(self) -> Iterator[TDataItem]:
        cursor = self.collection.find(self._filter_op)
        while docs_slice := list(islice(cursor, CHUNK_SIZE)):
            yield map_nested_in_place(convert_mongo_objs, docs_slice)


class CollectionLoaderParallell(CollectionLoader):
    @property
    def _sort_op(self) -> List[Optional[Tuple[str, int]]]:
        if not self.incremental or not self.last_value:
            return []
        if self.incremental.last_value_func is max:
            return [(self.cursor_field, ASCENDING)]
        elif self.incremental.last_value_func is min:
            return [(self.cursor_field, DESCENDING)]
        return []

    def _get_document_count(self) -> int:
        return self.collection.count_documents(filter=self._filter_op)

    def _create_batches(self) -> List[Dict[str, int]]:
        doc_count = self._get_document_count()
        return [
            dict(skip=sk, limit=CHUNK_SIZE) for sk in range(0, doc_count, CHUNK_SIZE)
        ]

    def _get_cursor(self) -> TCursor:
        cursor = self.collection.find(filter=self._filter_op)
        if self._sort_op:
            cursor = cursor.sort(self._sort_op)
        return cursor

    @dlt.defer
    def _run_batch(self, cursor: TCursor, batch: Dict[str, int]) -> TDataItem:
        cursor = cursor.clone()

        data = []
        for document in cursor.skip(batch["skip"]).limit(batch["limit"]):
            data.append(map_nested_in_place(convert_mongo_objs, document))
        return data

    def _get_all_batches(self) -> Iterator[TDataItem]:
        batches = self._create_batches()
        cursor = self._get_cursor()

        for batch in batches:
            yield self._run_batch(cursor=cursor, batch=batch)

    def load_documents(self) -> Iterator[TDataItem]:
        for document in self._get_all_batches():
            yield document


def collection_documents(
    client: TMongoClient,
    collection: TCollection,
    incremental: Optional[dlt.sources.incremental[Any]] = None,
    parallel: bool = False,
) -> Iterator[TDataItem]:
    """
    A DLT source which loads data from a Mongo database using PyMongo.
    Resources are automatically created for the collection.

    Args:
        client (MongoClient): The PyMongo client `pymongo.MongoClient` instance.
        collection (Collection): The collection `pymongo.collection.Collection` to load.
        incremental (Optional[dlt.sources.incremental[Any]]): The incremental configuration.
        parallel (bool): Option to enable parallel loading for the collection. Default is False.

    Returns:
        Iterable[DltResource]: A list of DLT resources for each collection to be loaded.
    """
    LoaderClass = CollectionLoaderParallell if parallel else CollectionLoader

    loader = LoaderClass(client, collection, incremental=incremental)
    for data in loader.load_documents():
        yield data

def client_from_credentials(connection_url: str) -> TMongoClient:
    client: TMongoClient = MongoClient(
        connection_url, uuidRepresentation="standard", tz_aware=True
    )
    return client


@configspec
class MongoDbCollectionConfiguration(BaseConfiguration):
    incremental: Optional[dlt.sources.incremental] = None  # type: ignore[type-arg]

@dlt.source
def mongodb(
    connection_url: str = dlt.secrets.value,
    database: Optional[str] = dlt.config.value,
    collection_names: Optional[List[str]] = dlt.config.value,
    incremental: Optional[dlt.sources.incremental] = None,  # type: ignore[type-arg]
    write_disposition: Optional[str] = dlt.config.value,
    parallel: Optional[bool] = dlt.config.value,
) -> Iterable[DltResource]:
    """
    A DLT source which loads data from a mongo database using PyMongo.
    Resources are automatically created for each collection in the database or from the given list of collection.

    Args:
        connection_url (str): Database connection_url.
        database (Optional[str]): Selected database name, it will use the default database if not passed.
        collection_names (Optional[List[str]]): The list of collections `pymongo.collection.Collection` to load.
        incremental (Optional[dlt.sources.incremental]): Option to enable incremental loading for the collection.
            E.g., `incremental=dlt.sources.incremental('updated_at', pendulum.parse('2022-01-01T00:00:00Z'))`
        write_disposition (str): Write disposition of the resource.
        parallel (Optional[bool]): Option to enable parallel loading for the collection. Default is False.
    Returns:
        Iterable[DltResource]: A list of DLT resources for each collection to be loaded.
    """

    # set up mongo client
    client = client_from_credentials(connection_url)
    if not database:
        mongo_database = client.get_default_database()
    else:
        mongo_database = client[database]

    # use provided collection or all conllections
    if not collection_names:
        collection_names = mongo_database.list_collection_names()

    collection_list = [mongo_database[collection] for collection in collection_names]

    for collection in collection_list:
        yield dlt.resource(  # type: ignore
            collection_documents,
            name=collection.name,
            primary_key="_id",
            write_disposition=write_disposition,
            spec=MongoDbCollectionConfiguration,
        )(client, collection, incremental=incremental, parallel=parallel)

try:
    load_data = mongodb("mongodb://root:example@mongo:27017/", "test", ["weather"])
    pipeline = dlt.pipeline(pipeline_name='pipeline_poc', destination='duckdb', dataset_name='weather_data', full_refresh=False)
    load_info = pipeline.run(load_data, table_name="users")
except:
    data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
    pipeline = dlt.pipeline(
        pipeline_name="quick_start", destination="duckdb", dataset_name="mydata"
    )
    load_info = pipeline.run(data, table_name="users")

print(load_info)
