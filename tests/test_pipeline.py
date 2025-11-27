from os.path import join

from freezegun import freeze_time
from hdx.utilities.compare import assert_files_same
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve

from hdx.scraper.portwatch.pipeline import Pipeline


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

                assert ports_dataset == {
                    "name": "ports",
                    "title": "Ports",
                    "dataset_date": "[2023-08-29T00:00:00 TO 2025-11-26T23:59:59]",
                    "tags": [
                        {
                            "name": "ports",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "trade",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "caveats": None,
                    "license_id": "hdx-other",
                    "license_other": "https://www.imf.org/en/about/copyright-and-terms",
                    "methodology": "Other",
                    "methodology_other": "[Tracking Trade from Space: An Application to Pacific Island "
                    "Countries](https://www.imf.org/en/publications/wp/issues/2021/08/20/tracking-trade-from-space-an-application-to-pacific-island-countries-464345)\n\n"
                    "[Nowcasting Global Trade from Space](https://www.imf.org/en/publications/wp/issues/2025/05/16/nowcasting-global-trade-from-space-566957)\n",
                    "dataset_source": "UN Global Platform; IMF PortWatch",
                    "groups": [{"name": "world"}],
                    "package_creator": "HDX Data Systems Team",
                    "private": False,
                    "maintainer": "fdbb8e79-f020-4039-ab3a-9adb482273b8",
                    "notes": "Daily count of port calls, estimates of import volumes and export "
                    "volumes (in metric tons) for ports in [country].",
                    "owner_org": "22945e84-d492-497f-9ffa-f9c6c394c04f",
                }

                ports_resources = ports_dataset.get_resources()
                assert ports_resources == [
                    {
                        "name": "ports.csv",
                        "description": (
                            "Global ports in CSV format. See variable descriptions "
                            "[here](https://portwatch.imf.org/datasets/acc668d199d1472abaaf2467133d4ca4/about)"
                        ),
                        "format": "csv",
                    },
                    {
                        "name": "ports.geojson",
                        "description": (
                            "Global ports in GeoJSON format. See variable descriptions "
                            "[here](https://portwatch.imf.org/datasets/acc668d199d1472abaaf2467133d4ca4/about)"
                        ),
                        "format": "geojson",
                    },
                ]

                for resource in ports_resources:
                    filename = resource["name"]
                    actual = join(tempdir, filename)
                    expected = join(input_dir, filename)
                    assert_files_same(actual, expected)

                # Test chokepoints dataset
                chokepoints_rows, chokepoints_geojson = pipeline.get_chokepoints()
                chokepoints_dataset = pipeline.generate_chokepoints_dataset(
                    chokepoints_rows, chokepoints_geojson
                )
                chokepoints_dataset.update_from_yaml(
                    path=join(config_dir, "hdx_dataset_static.yaml")
                )

                assert chokepoints_dataset == {
                    "name": "chokepoints",
                    "title": "Chokepoints",
                    "dataset_date": "[2023-09-08T00:00:00 TO 2025-11-26T23:59:59]",
                    "tags": [
                        {
                            "name": "ports",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "trade",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "caveats": None,
                    "license_id": "hdx-other",
                    "license_other": "https://www.imf.org/en/about/copyright-and-terms",
                    "methodology": "Other",
                    "methodology_other": "[Tracking Trade from Space: An Application to Pacific Island "
                    "Countries](https://www.imf.org/en/publications/wp/issues/2021/08/20/tracking-trade-from-space-an-application-to-pacific-island-countries-464345)\n\n"
                    "[Nowcasting Global Trade from Space](https://www.imf.org/en/publications/wp/issues/2025/05/16/nowcasting-global-trade-from-space-566957)\n",
                    "dataset_source": "UN Global Platform; IMF PortWatch",
                    "groups": [{"name": "world"}],
                    "package_creator": "HDX Data Systems Team",
                    "private": False,
                    "maintainer": "fdbb8e79-f020-4039-ab3a-9adb482273b8",
                    "notes": "Daily count of port calls, estimates of import volumes and export "
                    "volumes (in metric tons) for ports in [country].",
                    "owner_org": "22945e84-d492-497f-9ffa-f9c6c394c04f",
                }

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

                for resource in ports_resources:
                    filename = resource["name"]
                    actual = join(tempdir, filename)
                    expected = join(input_dir, filename)
                    assert_files_same(actual, expected)

                # Test daily chokepoints dataset
                daily_chokepoints_rows = pipeline.get_daily_chokepoints()
                daily_chokepoints_dataset = pipeline.generate_daily_chokepoints_dataset(
                    daily_chokepoints_rows
                )
                daily_chokepoints_dataset.update_from_yaml(
                    path=join(config_dir, "hdx_dataset_static.yaml")
                )

                assert daily_chokepoints_dataset == {
                    "name": "daily-chokepoint-transit-calls-and-trade-volume-estimates",
                    "title": "Daily Chokepoint Transit Calls and Trade Volume Estimates",
                    "dataset_date": "[2024-12-16T00:00:00 TO 2025-11-16T23:59:59]",
                    "tags": [
                        {
                            "name": "ports",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "trade",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "caveats": None,
                    "license_id": "hdx-other",
                    "license_other": "https://www.imf.org/en/about/copyright-and-terms",
                    "methodology": "Other",
                    "methodology_other": "[Tracking Trade from Space: An Application to Pacific Island "
                    "Countries](https://www.imf.org/en/publications/wp/issues/2021/08/20/tracking-trade-from-space-an-application-to-pacific-island-countries-464345)\n\n"
                    "[Nowcasting Global Trade from Space](https://www.imf.org/en/publications/wp/issues/2025/05/16/nowcasting-global-trade-from-space-566957)\n",
                    "dataset_source": "UN Global Platform; IMF PortWatch",
                    "groups": [{"name": "world"}],
                    "package_creator": "HDX Data Systems Team",
                    "private": False,
                    "maintainer": "fdbb8e79-f020-4039-ab3a-9adb482273b8",
                    "notes": "Daily count of port calls, estimates of import volumes and export "
                    "volumes (in metric tons) for ports in [country].",
                    "owner_org": "22945e84-d492-497f-9ffa-f9c6c394c04f",
                }

                daily_chokepoints_resources = daily_chokepoints_dataset.get_resources()
                assert daily_chokepoints_resources == [
                    {
                        "name": "daily-chokepoint-transit-calls-and-trade-volume-estimates.csv",
                        "description": (
                            "Daily chokepoint transit calls and preliminary transit trade volume estimates for 28 major chokepoints worldwide. See variable descriptions "
                            "[here](https://portwatch.imf.org/datasets/42132aa4e2fc4d41bdaf9a445f688931/about)"
                        ),
                        "format": "csv",
                    },
                ]

                for resource in daily_chokepoints_resources:
                    filename = resource["name"]
                    actual = join(tempdir, filename)
                    expected = join(input_dir, filename)
                    assert_files_same(actual, expected)

                # Test Disruptions dataset
                disruptions_rows, disruptions_geojson = pipeline.get_disruptions()
                disruptions_dataset = pipeline.generate_disruptions_dataset(
                    disruptions_rows, disruptions_geojson
                )
                disruptions_dataset.update_from_yaml(
                    path=join(config_dir, "hdx_dataset_static.yaml")
                )

                assert disruptions_dataset == {
                    "name": "disruptions",
                    "title": "Disruptions",
                    "dataset_date": "[2018-10-21T00:00:00 TO 2025-11-11T23:59:59]",
                    "tags": [
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
                    "caveats": None,
                    "license_id": "hdx-other",
                    "license_other": "https://www.imf.org/en/about/copyright-and-terms",
                    "methodology": "Other",
                    "methodology_other": "[Tracking Trade from Space: An Application to Pacific Island "
                    "Countries](https://www.imf.org/en/publications/wp/issues/2021/08/20/tracking-trade-from-space-an-application-to-pacific-island-countries-464345)\n\n"
                    "[Nowcasting Global Trade from Space](https://www.imf.org/en/publications/wp/issues/2025/05/16/nowcasting-global-trade-from-space-566957)\n",
                    "dataset_source": "UN Global Platform; IMF PortWatch",
                    "groups": [{"name": "world"}],
                    "package_creator": "HDX Data Systems Team",
                    "private": False,
                    "maintainer": "fdbb8e79-f020-4039-ab3a-9adb482273b8",
                    "notes": "Daily count of port calls, estimates of import volumes and export "
                    "volumes (in metric tons) for ports in [country].",
                    "owner_org": "22945e84-d492-497f-9ffa-f9c6c394c04f",
                }

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

                for resource in disruptions_resources:
                    filename = resource["name"]
                    actual = join(tempdir, filename)
                    expected = join(input_dir, filename)
                    assert_files_same(actual, expected)

                # Test Trade dataset
                country_code = "ABW"
                trade_data = pipeline.get_trade_data(country_code)
                trade_dataset = pipeline.generate_trade_dataset(
                    country_code, trade_data
                )
                trade_dataset.update_from_yaml(
                    path=join(config_dir, "hdx_dataset_static.yaml")
                )

                assert trade_dataset == {
                    "name": "aruba-daily-port-activity-data-and-trade-estimates",
                    "title": "Aruba: Daily Port Activity Data and Trade Estimates",
                    "dataset_date": "[2025-09-16T00:00:00 TO 2025-10-05T23:59:59]",
                    "tags": [
                        {
                            "name": "ports",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "trade",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "caveats": None,
                    "license_id": "hdx-other",
                    "license_other": "https://www.imf.org/en/about/copyright-and-terms",
                    "methodology": "Other",
                    "methodology_other": "[Tracking Trade from Space: An Application to Pacific Island "
                    "Countries](https://www.imf.org/en/publications/wp/issues/2021/08/20/tracking-trade-from-space-an-application-to-pacific-island-countries-464345)\n\n"
                    "[Nowcasting Global Trade from Space](https://www.imf.org/en/publications/wp/issues/2025/05/16/nowcasting-global-trade-from-space-566957)\n",
                    "dataset_source": "UN Global Platform; IMF PortWatch",
                    "groups": [{"name": "abw"}],
                    "package_creator": "HDX Data Systems Team",
                    "private": False,
                    "maintainer": "fdbb8e79-f020-4039-ab3a-9adb482273b8",
                    "notes": "Daily count of port calls, estimates of import volumes and export "
                    "volumes (in metric tons) for ports in [country].",
                    "owner_org": "22945e84-d492-497f-9ffa-f9c6c394c04f",
                }

                trade_resources = trade_dataset.get_resources()
                assert trade_resources == [
                    {
                        "description": (
                            "Daily port activity and preliminary trade volume estimates "
                            "for Aruba. See variable descriptions "
                            "[here](https://portwatch.imf.org/datasets/959214444157458aad969389b3ebe1a0_0/about)"
                        ),
                        "format": "csv",
                        "name": "aruba-daily-port-activity-data-and-trade-estimates.csv",
                    }
                ]

                for resource in trade_resources:
                    filename = resource["name"]
                    actual = join(tempdir, filename)
                    expected = join(input_dir, filename)
                    assert_files_same(actual, expected)
