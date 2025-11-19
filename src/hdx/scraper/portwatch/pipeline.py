#!/usr/bin/python
"""Portwatch scraper"""

import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, List, Optional

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.location.country import Country
from hdx.utilities.retriever import Retrieve
from slugify import slugify

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(self, configuration: Configuration, retriever: Retrieve, tempdir: str):
        self._configuration = configuration
        self._retriever = retriever
        self._tempdir = tempdir

    def get_ports(self) -> List:
        base_url = f"{self._configuration['base_url']}PortWatch_ports_database/FeatureServer/0/query"
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
            data = self._retriever.download_json(base_url, parameters=params)

            features = data.get("features", [])
            if not features:
                break

            all_data.extend(features)

            if len(features) < limit:
                break

            offset += limit
        return all_data

    def get_port_countries(self, features) -> List:
        iso3_set = set()

        for feature in features:
            attrs = feature.get("attributes", {})
            iso3 = attrs.get("ISO3")
            if iso3:
                iso3_set.add(iso3)

        return sorted(iso3_set)

    def get_trade_data(self, iso3: str) -> Dict:
        base_url = (
            f"{self._configuration['base_url']}Daily_Trade_Data/FeatureServer/0/query"
        )
        all_data = []
        offset = 0
        limit = 1000

        while True:
            params = {
                "where": f"ISO3='{iso3}'",  # "1=1",
                "outFields": "*",
                "outSR": 4326,
                "f": "json",
                "orderByFields": "OBJECTID",
                "resultOffset": offset,
                "resultRecordCount": limit,
            }
            data = self._retriever.download_json(base_url, parameters=params)

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

        # Group by year (use the 'year' field from the service)
        data_by_year = defaultdict(list)
        for row in all_data:
            year = row.get("year")
            if year is None:
                continue
            data_by_year[year].append(row)

        return data_by_year

    def generate_trade_dataset(
        self, country_code: str, data_by_year: Dict
    ) -> Optional[Dataset]:
        if not data_by_year:
            logger.warning(
                f"No trade data for country {country_code}, skipping dataset creation"
            )
            return None

        country_name = Country.get_country_name_from_iso3(country_code)
        dataset_title = f"{country_name}: Daily Port Activity Data and Trade Estimates"
        dataset_name = slugify(dataset_title)
        dataset_tags = self._configuration["tags"]

        # Flatten all rows to compute global min/max date
        all_rows = []
        for rows in data_by_year.values():
            all_rows.extend(rows)

        # Get date range
        min_date, max_date = self.get_date_range(all_rows)

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

        # Create one resource per year
        for year, rows in sorted(data_by_year.items(), reverse=True):
            if not rows:
                continue

            resource_name = f"{dataset_name}-{year}.csv"
            resource_data = {
                "name": resource_name,
                "description": (
                    f"Daily port activity and preliminary trade volume estimates "
                    f"for {country_name} in {year}."
                ),
            }

            # Get headers
            headers = list(rows[0].keys())

            dataset.generate_resource_from_iterable(
                headers=headers,
                iterable=rows,
                hxltags={},
                folder=self._tempdir,
                filename=resource_name,
                resourcedata=resource_data,
                quickcharts=None,
            )

        return dataset

    def generate_dataset(self) -> Optional[Dataset]:
        # To be generated
        dataset_name = None
        dataset_title = None
        dataset_time_period = None
        dataset_tags = None
        dataset_country_iso3 = None

        # Dataset info
        dataset = Dataset(
            {
                "name": dataset_name,
                "title": dataset_title,
            }
        )

        dataset.set_time_period(dataset_time_period)
        dataset.add_tags(dataset_tags)
        # Only if needed
        dataset.set_subnational(True)
        try:
            dataset.add_country_location(dataset_country_iso3)
        except HDXError:
            logger.error(f"Couldn't find country {dataset_country_iso3}, skipping")
            return

        # Add resources here

        return dataset

    def get_date_range(self, data):
        dates = []

        for row in data:
            d = row.get("date")

            if d is None:
                continue

            dates.append(d)

        if not dates:
            return None, None

        return min(dates), max(dates)
