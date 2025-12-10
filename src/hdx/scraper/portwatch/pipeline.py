#!/usr/bin/python
"""Portwatch scraper"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.data.resource import Resource
from hdx.location.country import Country
from hdx.utilities.downloader import Download
from hdx.utilities.retriever import Retrieve
from requests.exceptions import JSONDecodeError as RequestsJSONDecodeError
from slugify import slugify

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(self, configuration: Configuration, retriever: Retrieve, tempdir: str):
        self._configuration = configuration
        self._retriever = retriever
        self._tempdir = tempdir

    def get_ports(self) -> Tuple:
        base_url = f"{self._configuration['base_url']}/PortWatch_ports_database/FeatureServer/0/query"
        ports_rows = []
        geojson_features = []
        offset = 0
        limit = 1000

        while True:
            params = {
                "where": "1=1",
                "outFields": "*",
                "outSR": 4326,
                "f": "geojson",
                "orderByFields": "OBJECTID",
                "resultOffset": offset,
                "resultRecordCount": limit,
            }
            data = self._retriever.download_json(
                base_url, parameters=params, filename="ports.json"
            )

            features = data.get("features", [])
            if not features:
                break

            for feature in features:
                props = feature.get("properties", {}) or {}
                props.pop("ObjectId", None)

                # Create features for geojson
                feature["properties"] = props
                geojson_features.append(feature)

                # Create rows for csv
                ports_rows.append(props)

            if len(features) < limit:
                break

            offset += limit

        ports_geojson = {
            "type": "FeatureCollection",
            "features": geojson_features,
        }

        return ports_rows, ports_geojson

    def generate_ports_dataset(
        self, ports_rows: List, ports_geojson: Dict
    ) -> Optional[Dataset]:
        if not ports_rows:
            return None

        dataset_title = "Ports"
        dataset_name = slugify(dataset_title)
        dataset_tags = self._configuration["tags"]

        # Dataset info
        dataset = Dataset(
            {
                "name": dataset_name,
                "title": dataset_title,
            }
        )

        # Hardcode start date according to info in API metadata
        min_date = datetime(2023, 8, 29, 4, 8, 45)
        today = datetime.now()
        dataset.set_time_period(min_date, today)

        dataset.add_tags(dataset_tags)
        dataset.add_other_location("world")

        # Create csv resource
        csv_filename = f"{dataset_name}.csv"
        csv_resource_data = {
            "name": csv_filename,
            "description": (
                "Global ports in CSV format. See variable descriptions "
                "[here](https://portwatch.imf.org/datasets/acc668d199d1472abaaf2467133d4ca4/about)"
            ),
        }

        headers = list(ports_rows[0].keys())
        dataset.generate_resource_from_iterable(
            headers=headers,
            iterable=ports_rows,
            hxltags={},
            folder=self._tempdir,
            filename=csv_filename,
            resourcedata=csv_resource_data,
            quickcharts=None,
        )

        # Create geojson resource
        geojson_filename = f"{dataset_name}.geojson"
        geojson_path = os.path.join(self._tempdir, geojson_filename)

        # Create temp file for upload
        with open(geojson_path, "w", encoding="utf-8") as f:
            json.dump(ports_geojson, f)

        geojson_resource = Resource(
            {
                "name": geojson_filename,
                "description": (
                    "Global ports in GeoJSON format. See variable descriptions "
                    "[here](https://portwatch.imf.org/datasets/acc668d199d1472abaaf2467133d4ca4/about)"
                ),
                "format": "geojson",
            }
        )
        geojson_resource.set_file_to_upload(geojson_path)
        dataset.add_update_resource(geojson_resource)

        return dataset

    def get_port_countries(self, features) -> List:
        iso3_set = set()

        for feature in features:
            iso3 = feature.get("ISO3")
            if iso3:
                iso3_set.add(iso3)

        return sorted(iso3_set)

    def get_chokepoints(self) -> Tuple:
        base_url = f"{self._configuration['base_url']}/PortWatch_chokepoints_database/FeatureServer/0/query"
        chokepoints_rows = []
        geojson_features = []
        offset = 0
        limit = 1000

        while True:
            params = {
                "where": "1=1",
                "outFields": "*",
                "outSR": 4326,
                "f": "geojson",
                "orderByFields": "OBJECTID",
                "resultOffset": offset,
                "resultRecordCount": limit,
            }
            data = self._retriever.download_json(
                base_url, parameters=params, filename="chokepoints.json"
            )

            features = data.get("features", [])
            if not features:
                break

            for feature in features:
                props = feature.get("properties", {}) or {}
                props.pop("ObjectId", None)

                # Create features for geojson
                feature["properties"] = props
                geojson_features.append(feature)

                # Create rows for csv
                chokepoints_rows.append(props)

            if len(features) < limit:
                break

            offset += limit

        chokepoints_geojson = {
            "type": "FeatureCollection",
            "features": geojson_features,
        }

        return chokepoints_rows, chokepoints_geojson

    def generate_chokepoints_dataset(
        self, chokepoints_rows: List, chokepoints_geojson: Dict
    ) -> Optional[Dataset]:
        if not chokepoints_rows:
            return None

        dataset_title = "Chokepoints"
        dataset_name = slugify(dataset_title)
        dataset_tags = self._configuration["tags"]

        # Dataset info
        dataset = Dataset(
            {
                "name": dataset_name,
                "title": dataset_title,
            }
        )

        min_date = datetime(2023, 9, 8, 6, 0, 2)
        today = datetime.now()
        dataset.set_time_period(min_date, today)

        dataset.add_tags(dataset_tags)
        dataset.add_other_location("world")

        # Create csv resource
        csv_filename = f"{dataset_name}.csv"
        csv_resource_data = {
            "name": csv_filename,
            "description": (
                "Global chokepoints in CSV format. See variable descriptions "
                "[here](https://portwatch.imf.org/datasets/fa9a5800b0ee4855af8b2944ab1e07af/about)"
            ),
        }

        headers = list(chokepoints_rows[0].keys())
        dataset.generate_resource_from_iterable(
            headers=headers,
            iterable=chokepoints_rows,
            hxltags={},
            folder=self._tempdir,
            filename=csv_filename,
            resourcedata=csv_resource_data,
            quickcharts=None,
        )

        # Create geojson resource
        geojson_filename = f"{dataset_name}.geojson"
        geojson_path = os.path.join(self._tempdir, geojson_filename)

        # Create temp file for upload
        with open(geojson_path, "w", encoding="utf-8") as f:
            json.dump(chokepoints_geojson, f)

        geojson_resource = Resource(
            {
                "name": geojson_filename,
                "description": (
                    "Global chokepoints in GeoJSON format. See variable descriptions "
                    "[here](https://portwatch.imf.org/datasets/fa9a5800b0ee4855af8b2944ab1e07af/about)"
                ),
                "format": "geojson",
            }
        )
        geojson_resource.set_file_to_upload(geojson_path)
        dataset.add_update_resource(geojson_resource)

        return dataset

    def get_daily_chokepoints(self) -> List:
        base_url = f"{self._configuration['base_url']}/Daily_Chokepoints_Data/FeatureServer/0/query"
        all_data = []
        offset = 0
        limit = 1000

        while True:
            params = {
                "where": "1=1",
                "outFields": "*",
                "outSR": 4326,
                "f": "json",
                "orderByFields": "OBJECTID",
                "resultOffset": offset,
                "resultRecordCount": limit,
            }
            data = self._retriever.download_json(
                base_url, parameters=params, filename="daily_chokepoints.json"
            )

            features = data.get("features", [])
            if not features:
                break

            for feature in features:
                attrs = feature.get("attributes", {})
                all_data.append(attrs)

            if len(features) < limit:
                break

            offset += limit

        for row in all_data:
            row["date"] = datetime.fromtimestamp(row["date"] / 1000, tz=timezone.utc)
            row.pop("ObjectId", None)

        all_data = sorted(all_data, key=lambda x: x["date"], reverse=True)
        return all_data

    def generate_daily_chokepoints_dataset(
        self, daily_chokepoints_rows: List
    ) -> Optional[Dataset]:
        if not daily_chokepoints_rows:
            return None

        dataset_title = "Daily Chokepoint Transit Calls and Shipment Volume Estimates"
        dataset_name = slugify(dataset_title)
        dataset_tags = self._configuration["tags"]

        # Get date range
        min_date, max_date = self.get_date_range(daily_chokepoints_rows)

        # Dataset info
        dataset = Dataset(
            {
                "name": dataset_name,
                "title": dataset_title,
            }
        )

        dataset.set_time_period(min_date, max_date)
        dataset.add_tags(dataset_tags)
        dataset.add_other_location("world")

        # Create csv resource
        csv_filename = f"{dataset_name}.csv"
        csv_resource_data = {
            "name": csv_filename,
            "description": (
                "Daily chokepoint transit calls and preliminary transit shipment volume estimates for 28 major chokepoints worldwide. See variable descriptions "
                "[here](https://portwatch.imf.org/datasets/42132aa4e2fc4d41bdaf9a445f688931/about)"
            ),
        }

        headers = list(daily_chokepoints_rows[0].keys())
        dataset.generate_resource_from_iterable(
            headers=headers,
            iterable=daily_chokepoints_rows,
            hxltags={},
            folder=self._tempdir,
            filename=csv_filename,
            resourcedata=csv_resource_data,
            quickcharts=None,
        )

        return dataset

    def get_daily_ports(self, iso3: str) -> List:
        base_url = (
            f"{self._configuration['base_url']}/Daily_Trade_Data/FeatureServer/0/query"
        )
        all_data = []
        offset = 0
        limit = 1000

        while True:
            params = {
                "where": f"ISO3='{iso3}'",
                "outFields": "*",
                "outSR": 4326,
                "f": "json",
                "orderByFields": "OBJECTID",
                "resultOffset": offset,
                "resultRecordCount": limit,
            }
            # data = self._retriever.download_json(
            #     base_url, parameters=params, filename="daily_ports.json"
            # )

            try:
                data = self._retriever.download_json(
                    base_url, parameters=params, filename="daily_ports.json"
                )
            except RequestsJSONDecodeError as exc:
                # Extra debug logging for Jenkins so we can see what ArcGIS is returning
                logger.error(
                    "JSONDecodeError when calling Daily_Trade_Data endpoint "
                    "for iso3=%s, offset=%s, limit=%s",
                    iso3,
                    offset,
                    limit,
                )
                with Download(user_agent="portwatch-debug") as d:
                    d.download(base_url, parameters=params)
                    resp = d.response
                    logger.error(
                        "DEBUG Daily_Trade_Data status_code: %s", resp.status_code
                    )
                    logger.error(
                        "DEBUG Daily_Trade_Data content-type: %s",
                        resp.headers.get("content-type"),
                    )
                    body_preview = resp.text[:1000]  # avoid flooding Jenkins logs
                    logger.error(
                        "DEBUG Daily_Trade_Data body (first 1000 chars): %r",
                        body_preview,
                    )

                    # Second attempt: try to parse the debug response as JSON
                    try:
                        data = resp.json()
                    except Exception as exc2:
                        logger.error(
                            "Second JSON parse attempt also failed for iso3=%s, "
                            "offset=%s, limit=%s; giving up.",
                            iso3,
                            offset,
                            limit,
                        )
                        # Re-raise the original error so the scraper clearly fails
                        raise exc from exc2

            features = data.get("features", [])
            if not features:
                break

            for feature in features:
                attrs = feature.get("attributes", {})
                all_data.append(attrs)

            if len(features) < limit:
                break

            offset += limit

        for row in all_data:
            row["date"] = datetime.fromtimestamp(row["date"] / 1000, tz=timezone.utc)
            row.pop("ObjectId", None)

        all_data = sorted(all_data, key=lambda x: x["date"], reverse=True)
        return all_data

    def generate_daily_ports_dataset(
        self, country_code: str, data_by_country: List
    ) -> Optional[Dataset]:
        if not data_by_country:
            logger.warning(
                f"No daily ports data for country {country_code}, skipping dataset creation"
            )
            return None

        country_name = Country.get_country_name_from_iso3(country_code)
        dataset_title = (
            f"{country_name}: Daily Port Activity Data and Shipment Estimates"
        )
        dataset_name = slugify(dataset_title)
        dataset_tags = self._configuration["tags"]

        # Get date range
        min_date, max_date = self.get_date_range(data_by_country)

        # Dataset info
        dataset = Dataset(
            {
                "name": dataset_name,
                "title": dataset_title,
            }
        )

        dataset.set_time_period(min_date, max_date)
        dataset.add_tags(dataset_tags)

        try:
            dataset.add_country_location(country_code)
        except HDXError:
            logger.error(f"Couldn't find country {country_code}, skipping")
            return

        # Create one resource per country
        resource_name = f"{dataset_name}.csv"
        resource_data = {
            "name": resource_name,
            "description": (
                f"Daily port activity and preliminary shipment volume estimates "
                f"for {country_name}. See variable descriptions [here](https://portwatch.imf.org/datasets/959214444157458aad969389b3ebe1a0_0/about)"
            ),
        }

        # Get headers
        headers = list(data_by_country[0].keys())
        dataset.generate_resource_from_iterable(
            headers=headers,
            iterable=data_by_country,
            hxltags={},
            folder=self._tempdir,
            filename=resource_name,
            resourcedata=resource_data,
            quickcharts=None,
        )

        return dataset

    def get_disruptions(self) -> Tuple:
        base_url = f"{self._configuration['base_url']}/portwatch_disruptions_database/FeatureServer/0/query"
        disruptions_rows = []
        geojson_features = []
        offset = 0
        limit = 1000

        while True:
            params = {
                "where": "1=1",
                "outFields": "*",
                "outSR": 4326,
                "f": "geojson",
                "orderByFields": "OBJECTID",
                "resultOffset": offset,
                "resultRecordCount": limit,
            }
            data = self._retriever.download_json(
                base_url, parameters=params, filename="disruptions.json"
            )

            features = data.get("features", [])
            if not features:
                break

            for feature in features:
                props = feature.get("properties", {}) or {}

                # Create features for geojson
                feature["properties"] = props
                geojson_features.append(feature)

                # Create rows for csv
                disruptions_rows.append(props)

            if len(features) < limit:
                break

            offset += limit

        disruptions_geojson = {
            "type": "FeatureCollection",
            "features": geojson_features,
        }

        return disruptions_rows, disruptions_geojson

    def generate_disruptions_dataset(
        self, disruptions_rows: List, disruptions_geojson: Dict
    ) -> Optional[Dataset]:
        if not disruptions_rows:
            return None

        dataset_title = "Disruptions"
        dataset_name = slugify(dataset_title)
        dataset_tags = self._configuration["disruptions_tags"]

        # Create temp file for upload
        geojson_filename = f"{dataset_name}.geojson"
        geojson_path = os.path.join(self._tempdir, geojson_filename)
        with open(geojson_path, "w", encoding="utf-8") as f:
            json.dump(disruptions_geojson, f)

        # Format date rows
        for row in disruptions_rows:
            row["fromdate"] = datetime.fromtimestamp(
                row["fromdate"] / 1000, tz=timezone.utc
            )
            if row.get("todate") is None:
                row["todate"] = row["fromdate"]
            else:
                row["todate"] = datetime.fromtimestamp(
                    row["todate"] / 1000, tz=timezone.utc
                )
            row.pop("ObjectId", None)

        # Get date range
        min_date, max_date = self.get_date_range(disruptions_rows)

        # Dataset info
        dataset = Dataset(
            {
                "name": dataset_name,
                "title": dataset_title,
            }
        )

        dataset.set_time_period(min_date, max_date)
        dataset.add_tags(dataset_tags)
        dataset.add_other_location("world")

        # Create csv resource
        csv_filename = f"{dataset_name}.csv"
        csv_resource_data = {
            "name": csv_filename,
            "description": (
                "Dataset identifying ports and chokepoints at risk by intersecting GDACS data. See variable descriptions "
                "[here](https://portwatch.imf.org/datasets/d9b37bf4b2104c85aebdcc0c1d8a2ab7_0/about)"
            ),
        }

        headers = list(disruptions_rows[0].keys())
        dataset.generate_resource_from_iterable(
            headers=headers,
            iterable=disruptions_rows,
            hxltags={},
            folder=self._tempdir,
            filename=csv_filename,
            resourcedata=csv_resource_data,
            quickcharts=None,
        )

        # Create geojson resource
        geojson_resource = Resource(
            {
                "name": geojson_filename,
                "description": (
                    "Dataset in GeoJSON format identifying ports and chokepoints at risk by intersecting GDACS data. See variable descriptions "
                    "[here](https://portwatch.imf.org/datasets/acc668d199d1472abaaf2467133d4ca4/about)"
                ),
                "format": "geojson",
            }
        )
        geojson_resource.set_file_to_upload(geojson_path)
        dataset.add_update_resource(geojson_resource)

        return dataset

    def get_date_range(self, data):
        from_dates = []
        to_dates = []

        for row in data:
            if "fromdate" in row and "todate" in row:
                if row.get("fromdate") is not None:
                    from_dates.append(row["fromdate"])
                if row.get("todate") is not None:
                    to_dates.append(row["todate"])
            else:
                d = row.get("date")
                if d is not None:
                    from_dates.append(d)
                    to_dates.append(d)

        if not from_dates or not to_dates:
            return None, None

        return min(from_dates), max(to_dates)
