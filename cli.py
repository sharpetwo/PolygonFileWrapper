import argparse
import datetime as dt

from polygon_wrapper import PolygonEndpoint, PolygonFileWrapper


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--endpoint",
        choices=["day", "minutes", "trades", "quotes"],
        help='Type of data to download.',
        required=True
    )
    parser.add_argument(
        "--market",
        choices=["options", "stocks"],
        help='Asset class to download',
        required=True
    )
    parser.add_argument(
        "--start_date", help="Start date (inclusive), YYYYMMDD format.", required=True
    )
    parser.add_argument(
        "--end_date", help="End date (inclusive), YYYYMMDD format.", required=False
    )
    parser.add_argument(
        '--output_dir',
        type=str,
        help='The directory to save output files.',
        required=True
    )
    args = parser.parse_args()

    wrapper = PolygonFileWrapper()
    endpoint = PolygonEndpoint[args.endpoint.upper()]
    start_date = dt.datetime.strptime(args.start_date, "%Y%m%d").date()
    end_date = dt.datetime.strptime(args.end_date, "%Y%m%d").date() if args.end_date else None
    wrapper.download_and_save_options(endpoint, start_date, end_date, dir=args.output_dir, clean=True)
