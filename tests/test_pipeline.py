from os.path import join

from freezegun import freeze_time
from hdx.utilities.compare import assert_files_same
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve

from hdx.scraper.portwatch.pipeline import Pipeline

COMMON_TAGS = [
    {
        "name": "ports",
        "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
    },
    {
        "name": "trade",
        "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
    },
]

METHODOLOGY = (
    "[Tracking Trade from Space: An Application to Pacific Island "
    "Countries](https://www.imf.org/en/publications/wp/issues/2021/08/20/"
    "tracking-trade-from-space-an-application-to-pacific-island-countries-464345)\n\n"
    "[Nowcasting Global Trade from Space]"
    "(https://www.imf.org/en/publications/wp/issues/2025/05/16/"
    "nowcasting-global-trade-from-space-566957)\n"
)

NOTES = (
    "Daily count of port calls, estimates of incoming shipment volumes and outgoing shipment "
    "volumes (in metric tons) for ports in [country]."
)

COMMON_FIELDS = {
    "caveats": None,
    "dataset_source": "UN Global Platform; IMF PortWatch",
    "license_id": "hdx-other",
    "license_other": "https://www.imf.org/en/about/copyright-and-terms",
    "maintainer": "1d7ceaf2-06c0-49f8-a871-fc974a07ed75",
    "methodology": "Other",
    "methodology_other": METHODOLOGY,
    "notes": NOTES,
    "owner_org": "22945e84-d492-497f-9ffa-f9c6c394c04f",
    "package_creator": "HDX Data Systems Team",
    "private": False,
}


def assert_dataset(*, name, title, dataset_date, tags, groups):
    return {
        **COMMON_FIELDS,
        "name": name,
        "title": title,
        "dataset_date": dataset_date,
        "tags": tags,
        "groups": groups,
    }


def assert_resources(tempdir, input_dir, resources):
    for resource in resources:
        filename = resource["name"]
        actual = join(tempdir, filename)
        expected = join(input_dir, filename)
        assert_files_same(actual, expected)


@freeze_time("2025-11-26")
class TestPipeline:
    def test_pipeline(self, configuration, fixtures_dir, input_dir, config_dir):
        with temp_dir(
            "TestPortwatch",
            delete_on_success=True,
            delete_on_failure=False,
        ) as tempdir:
            with Download(user_agent="test") as downloader:
                retriever = Retrieve(
                    downloader=downloader,
                    fallback_dir=tempdir,
                    saved_dir=input_dir,
                    temp_dir=tempdir,
                    save=False,
                    use_saved=True,
                )
                pipeline = Pipeline(configuration, retriever, tempdir)

                # Test ports dataset
                ports_rows, ports_geojson = pipeline.get_ports()
                ports_dataset = pipeline.generate_ports_dataset(
                    ports_rows, ports_geojson
                )
                ports_dataset.update_from_yaml(
                    path=join(config_dir, "hdx_dataset_static.yaml")
                )

                assert ports_dataset == assert_dataset(
                    name="ports",
                    title="Ports",
                    dataset_date="[2023-08-29T00:00:00 TO 2025-11-26T23:59:59]",
                    tags=COMMON_TAGS,
                    groups=[{"name": "world"}],
                )

                ports_resources = ports_dataset.get_resources()
                assert ports_resources == [
                    {
                        "name": "ports.csv",
                        "description": (
                            "Global ports in CSV format. See variable descriptions "
                            "[here](https://portwatch.imf.org/datasets/"
                            "acc668d199d1472abaaf2467133d4ca4/about)"
                        ),
                        "format": "csv",
                    },
                    {
                        "name": "ports.geojson",
                        "description": (
                            "Global ports in GeoJSON format. See variable descriptions "
                            "[here](https://portwatch.imf.org/datasets/"
                            "acc668d199d1472abaaf2467133d4ca4/about)"
                        ),
                        "format": "geojson",
                    },
                ]
                assert_resources(tempdir, input_dir, ports_resources)

                # Test chokepoints dataset
                chokepoints_rows, chokepoints_geojson = pipeline.get_chokepoints()
                chokepoints_dataset = pipeline.generate_chokepoints_dataset(
                    chokepoints_rows, chokepoints_geojson
                )
                chokepoints_dataset.update_from_yaml(
                    path=join(config_dir, "hdx_dataset_static.yaml")
                )

                assert chokepoints_dataset == assert_dataset(
                    name="chokepoints",
                    title="Chokepoints",
                    dataset_date="[2023-09-08T00:00:00 TO 2025-11-26T23:59:59]",
                    tags=COMMON_TAGS,
                    groups=[{"name": "world"}],
                )

                chokepoints_resources = chokepoints_dataset.get_resources()
                assert chokepoints_resources == [
                    {
                        "name": "chokepoints.csv",
                        "description": (
                            "Global chokepoints in CSV format. See variable descriptions "
                            "[here](https://portwatch.imf.org/datasets/fa9a5800b0ee4855af8b2944ab1e07af/about)"
                        ),
                        "format": "csv",
                    },
                    {
                        "name": "chokepoints.geojson",
                        "description": (
                            "Global chokepoints in GeoJSON format. See variable descriptions "
                            "[here](https://portwatch.imf.org/datasets/fa9a5800b0ee4855af8b2944ab1e07af/about)"
                        ),
                        "format": "geojson",
                    },
                ]
                assert_resources(tempdir, input_dir, chokepoints_resources)

                # Test daily chokepoints dataset
                daily_chokepoints_rows = pipeline.get_daily_chokepoints()
                daily_chokepoints_dataset = pipeline.generate_daily_chokepoints_dataset(
                    daily_chokepoints_rows
                )
                daily_chokepoints_dataset.update_from_yaml(
                    path=join(config_dir, "hdx_dataset_static.yaml")
                )

                assert daily_chokepoints_dataset == assert_dataset(
                    name="daily-chokepoint-transit-calls-and-shipment-volume-estimates",
                    title="Daily Chokepoint Transit Calls and Shipment Volume Estimates",
                    dataset_date="[2024-12-16T00:00:00 TO 2025-11-16T23:59:59]",
                    tags=COMMON_TAGS,
                    groups=[{"name": "world"}],
                )

                daily_chokepoints_resources = daily_chokepoints_dataset.get_resources()
                assert daily_chokepoints_resources == [
                    {
                        "name": "daily-chokepoint-transit-calls-and-shipment-volume-estimates.csv",
                        "description": (
                            "Daily chokepoint transit calls and preliminary transit shipment volume estimates for 28 major chokepoints worldwide. See variable descriptions "
                            "[here](https://portwatch.imf.org/datasets/42132aa4e2fc4d41bdaf9a445f688931/about)"
                        ),
                        "format": "csv",
                    },
                ]
                assert_resources(tempdir, input_dir, daily_chokepoints_resources)

                # Test Disruptions dataset
                disruptions_rows, disruptions_geojson = pipeline.get_disruptions()
                disruptions_dataset = pipeline.generate_disruptions_dataset(
                    disruptions_rows, disruptions_geojson
                )
                disruptions_dataset.update_from_yaml(
                    path=join(config_dir, "hdx_dataset_static.yaml")
                )

                assert disruptions_dataset == assert_dataset(
                    name="disruptions",
                    title="Disruptions",
                    dataset_date="[2018-10-21T00:00:00 TO 2025-11-11T23:59:59]",
                    groups=[{"name": "world"}],
                    tags=[
                        {
                            "name": "cyclones-hurricanes-typhoons",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "earthquake-tsunami",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "flooding",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "natural disasters",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "ports",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "trade",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                )

                disruptions_resources = disruptions_dataset.get_resources()
                assert disruptions_resources == [
                    {
                        "name": "disruptions.csv",
                        "description": (
                            "Dataset identifying ports and chokepoints at risk by intersecting GDACS data. See variable descriptions "
                            "[here](https://portwatch.imf.org/datasets/d9b37bf4b2104c85aebdcc0c1d8a2ab7_0/about)"
                        ),
                        "format": "csv",
                    },
                    {
                        "name": "disruptions.geojson",
                        "description": (
                            "Dataset in GeoJSON format identifying ports and chokepoints at risk by intersecting GDACS data. See variable descriptions "
                            "[here](https://portwatch.imf.org/datasets/acc668d199d1472abaaf2467133d4ca4/about)"
                        ),
                        "format": "geojson",
                    },
                ]
                assert_resources(tempdir, input_dir, disruptions_resources)

                # Test Daily Ports dataset
                country_code = "ABW"
                daily_ports_data = pipeline.get_daily_ports(country_code)
                daily_ports_dataset = pipeline.generate_daily_ports_dataset(
                    country_code, daily_ports_data
                )
                daily_ports_dataset.update_from_yaml(
                    path=join(config_dir, "hdx_dataset_static.yaml")
                )

                assert daily_ports_dataset == assert_dataset(
                    name="aruba-daily-port-activity-data-and-shipment-estimates",
                    title="Aruba: Daily Port Activity Data and Shipment Estimates",
                    dataset_date="[2025-09-16T00:00:00 TO 2025-10-05T23:59:59]",
                    tags=COMMON_TAGS,
                    groups=[{"name": "abw"}],
                )

                daily_ports_resources = daily_ports_dataset.get_resources()
                assert daily_ports_resources == [
                    {
                        "description": (
                            "Daily port activity and preliminary shipment volume estimates "
                            "for Aruba. See variable descriptions "
                            "[here](https://portwatch.imf.org/datasets/959214444157458aad969389b3ebe1a0_0/about)"
                        ),
                        "format": "csv",
                        "name": "aruba-daily-port-activity-data-and-shipment-estimates.csv",
                    }
                ]
                assert_resources(tempdir, input_dir, daily_ports_resources)
